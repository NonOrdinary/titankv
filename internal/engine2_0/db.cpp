#include "db.hpp"
#include "sstable_iterator.hpp"
#include "internal_key.hpp"
#include <filesystem>
#include <chrono>
#include <algorithm>
#include <queue>
#include <thread>
#include <iostream>

struct HeapItem {
    std::vector<uint8_t> InternalKey;
    std::vector<uint8_t> Value;
    size_t IterIdx;
};

struct HeapItemCompare {
    bool operator()(const HeapItem* a, const HeapItem* b) const {
        return CompareInternalKeys(a->InternalKey, b->InternalKey) > 0;
    }
};

DB::DB(const std::string& dir)
    : dir_(dir),
      activeMemTable_(std::make_shared<MemTable>()),
      immutableMemTable_(nullptr),
      isCompacting_(false),
      maxMemtableSize_(4 * 1024 * 1024),
      nextSeqNum_(0) {}

DB::~DB() {
    Close();
}

std::unique_ptr<DB> DB::Open(const std::string& dir) {
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);

    std::string manifestPath = dir + "/MANIFEST.log";
    auto activeRecords = RecoverManifest(manifestPath);

    std::vector<std::shared_ptr<SSTableReader>> sstables;
    for (const auto& rec : activeRecords) {
        auto reader = SSTableReader::Open(rec.path);
        if (reader) {
            sstables.push_back(reader);
        }
    }

    auto manifest = Manifest::Open(manifestPath);
    if (!manifest) {
        return nullptr;
    }

    std::string walPath = dir + "/active.wal";
    auto wal = WAL::Open(walPath);
    if (!wal) {
        return nullptr;
    }

    auto mt = std::make_shared<MemTable>();
    uint64_t maxWalSeq = wal->Recover(mt.get());

    auto db = std::unique_ptr<DB>(new DB(dir));
    db->activeMemTable_ = mt;
    db->activeWAL_ = std::move(wal);
    db->manifest_ = std::move(manifest);
    db->sstables_ = std::move(sstables);

    uint64_t baseSeq = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();

    if (maxWalSeq >= baseSeq) {
        baseSeq = maxWalSeq + 1;
    }
    db->nextSeqNum_ = baseSeq;

    db->flushThread_ = std::thread(&DB::flushWorker, db.get());

    return db;
}

bool DB::Put(const std::string& key, const std::vector<uint8_t>& value) {
    std::vector<uint8_t> userKey(key.begin(), key.end());
    std::unique_lock<std::shared_mutex> lock(mu_);

    while (immutableMemTable_ != nullptr) {
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        lock.lock();
    }

    uint64_t seqNum = ++nextSeqNum_;
    std::vector<uint8_t> internalKey = EncodeInternalKey(userKey, seqNum, TypePut);

    if (!activeWAL_->WriteRecord(internalKey, value)) {
        return false;
    }

    activeMemTable_->Put(userKey, value, seqNum);

    if (activeMemTable_->ApproximateSize() >= maxMemtableSize_) {
        triggerFlush(); // triggerFlush unlocks mu_ internally
    }

    return true;
}

bool DB::Delete(const std::string& key) {
    std::vector<uint8_t> userKey(key.begin(), key.end());
    std::unique_lock<std::shared_mutex> lock(mu_);

    while (immutableMemTable_ != nullptr) {
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        lock.lock();
    }

    uint64_t seqNum = ++nextSeqNum_;
    std::vector<uint8_t> internalKey = EncodeInternalKey(userKey, seqNum, TypeDelete);

    if (!activeWAL_->WriteRecord(internalKey, {})) {
        return false;
    }

    activeMemTable_->Delete(userKey, seqNum);

    if (activeMemTable_->ApproximateSize() >= maxMemtableSize_) {
        triggerFlush(); // triggerFlush unlocks mu_ internally
    }

    return true;
}

bool DB::Get(const std::string& key, std::vector<uint8_t>& val, bool& exists) const {
    return GetAt(key, std::numeric_limits<uint64_t>::max(), val, exists);
}

bool DB::GetAt(const std::string& key, uint64_t targetSeqNum, std::vector<uint8_t>& val, bool& exists) const {
    std::vector<uint8_t> userKey(key.begin(), key.end());
    std::shared_lock<std::shared_mutex> lock(mu_);

    std::vector<uint8_t> valBytes;
    bool isDeleted = false;

    if (activeMemTable_->Get(userKey, targetSeqNum, valBytes, isDeleted)) {
        if (isDeleted) {
            exists = false;
            return true;
        }
        val = std::move(valBytes);
        exists = true;
        return true;
    }

    if (immutableMemTable_) {
        if (immutableMemTable_->Get(userKey, targetSeqNum, valBytes, isDeleted)) {
            if (isDeleted) {
                exists = false;
                return true;
            }
            val = std::move(valBytes);
            exists = true;
            return true;
        }
    }

    for (int i = static_cast<int>(sstables_.size()) - 1; i >= 0; i--) {
        auto sst = sstables_[i];
        if (key < sst->GetMinKey() || key > sst->GetMaxKey()) {
            continue;
        }

        if (sst->Get(userKey, targetSeqNum, valBytes, isDeleted)) {
            if (isDeleted) {
                exists = false;
                return true;
            }
            val = std::move(valBytes);
            exists = true;
            return true;
        }
    }

    exists = false;
    return true;
}

void DB::triggerFlush() {
    immutableMemTable_ = activeMemTable_;
    activeMemTable_ = std::make_shared<MemTable>();

    std::string newWALPath = dir_ + "/wal_" + std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count()) + ".log";
    
    auto frozenWAL = std::move(activeWAL_);
    activeWAL_ = WAL::Open(newWALPath);

    auto frozenMem = immutableMemTable_;
    
    mu_.unlock();

    flushChan_.Push({frozenMem, std::move(frozenWAL)});
}

void DB::flushWorker() {
    FlushTask task;
    while (flushChan_.Pop(task)) {
        if (!task.mt || !task.wal) continue;

        std::string sstPath = dir_ + "/sst_" + std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()) + ".sst";

        try {
            SSTableBuilder builder(sstPath);
            task.mt->Iterate([&builder](const std::vector<uint8_t>& ik, const std::vector<uint8_t>& val) {
                builder.Add(ik, val);
            });
            builder.Finish();
        } catch (const std::exception& e) {
            std::cerr << "Flush failed: " << e.what() << std::endl;
            continue;
        }

        auto reader = SSTableReader::Open(sstPath);
        if (!reader) {
            std::cerr << "Failed to open flushed SSTable: " << sstPath << std::endl;
            continue;
        }

        std::unique_lock<std::shared_mutex> lock(mu_);
        manifest_->Append(ManifestRecord{"ADD", sstPath, reader->GetMinKey(), reader->GetMaxKey()});
        sstables_.push_back(reader);
        immutableMemTable_ = nullptr;

        size_t numTables = sstables_.size();
        bool isComp = isCompacting_;
        if (numTables >= 4 && !isComp) {
            isCompacting_ = true;
            std::thread(&DB::Compact, this).detach();
        }
        lock.unlock();

        task.wal->Close();
        std::error_code ec;
        std::filesystem::remove(task.wal->GetPath(), ec);
    }
}

bool DB::Compact() {
    auto cleanup = [this]() {
        std::unique_lock<std::shared_mutex> lock(mu_);
        isCompacting_ = false;
    };

    uint64_t watermark = std::chrono::duration_cast<std::chrono::nanoseconds>(
        (std::chrono::system_clock::now() - std::chrono::hours(24)).time_since_epoch()
    ).count();

    std::shared_lock<std::shared_mutex> rlock(mu_);
    if (sstables_.size() < 4) {
        rlock.unlock();
        cleanup();
        return true;
    }
    std::vector<std::shared_ptr<SSTableReader>> tablesToMerge(sstables_.begin(), sstables_.begin() + 4);
    rlock.unlock();

    std::vector<std::unique_ptr<SSTableIterator>> iterators;
    std::priority_queue<HeapItem*, std::vector<HeapItem*>, HeapItemCompare> pq;

    for (size_t i = 0; i < tablesToMerge.size(); i++) {
        auto iter = std::make_unique<SSTableIterator>(tablesToMerge[i]->GetPath(), tablesToMerge[i]->GetBloomStartOffset());
        IteratorKV kv;
        if (iter->Next(kv)) {
            pq.push(new HeapItem{std::move(kv.InternalKey), std::move(kv.Value), i});
        }
        iterators.push_back(std::move(iter));
    }

    std::string newSSTPath = dir_ + "/sst_compacted_" + std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count()) + ".sst";

    std::unique_ptr<SSTableBuilder> builder;
    try {
        builder = std::make_unique<SSTableBuilder>(newSSTPath);
    } catch (...) {
        while (!pq.empty()) {
            delete pq.top();
            pq.pop();
        }
        cleanup();
        return false;
    }

    while (!pq.empty()) {
        HeapItem* top = pq.top();
        pq.pop();

        std::vector<uint8_t> userKey;
        uint64_t seqNum = 0;
        uint8_t keyType = 0;
        ParseInternalKey(top->InternalKey, userKey, seqNum, keyType);

        bool keepVersion = true;
        if (seqNum <= watermark && keyType == TypeDelete) {
            keepVersion = false;
        }

        if (keepVersion) {
            builder->Add(top->InternalKey, top->Value);
        }

        size_t iterIdx = top->IterIdx;
        delete top;

        while (true) {
            IteratorKV nextKV;
            if (!iterators[iterIdx]->Next(nextKV)) {
                break;
            }

            std::vector<uint8_t> nUserKey;
            uint64_t nSeqNum = 0;
            uint8_t nKeyType = 0;
            ParseInternalKey(nextKV.InternalKey, nUserKey, nSeqNum, nKeyType);

            if (nUserKey != userKey) {
                pq.push(new HeapItem{std::move(nextKV.InternalKey), std::move(nextKV.Value), iterIdx});
                break;
            }

            if (seqNum <= watermark) {
                continue;
            }

            pq.push(new HeapItem{std::move(nextKV.InternalKey), std::move(nextKV.Value), iterIdx});
            break;
        }
    }

    builder->Finish();

    auto newReader = SSTableReader::Open(newSSTPath);

    std::unique_lock<std::shared_mutex> lock(mu_);
    sstables_.erase(sstables_.begin(), sstables_.begin() + 4);
    sstables_.insert(sstables_.begin(), newReader);

    manifest_->Append(ManifestRecord{"ADD", newSSTPath, newReader->GetMinKey(), newReader->GetMaxKey()});
    for (const auto& oldTable : tablesToMerge) {
        manifest_->Append(ManifestRecord{"REMOVE", oldTable->GetPath(), "", ""});
    }
    isCompacting_ = false;
    lock.unlock();

    std::thread([tablesToMerge]() {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        for (const auto& oldTable : tablesToMerge) {
            oldTable->Close();
            std::error_code ec;
            std::filesystem::remove(oldTable->GetPath(), ec);
        }
    }).detach();

    return true;
}

void DB::Close() {
    flushChan_.Close();
    if (flushThread_.joinable()) {
        flushThread_.join();
    }

    std::unique_lock<std::shared_mutex> lock(mu_);
    if (activeWAL_) {
        activeWAL_->Close();
    }
    if (manifest_) {
        manifest_->Close();
    }
    for (auto& sst : sstables_) {
        sst->Close();
    }
}

void DB::SetMaxMemTableSize(size_t size) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    maxMemtableSize_ = size;
}

uint64_t DB::GetNextSeqNum() const {
    return nextSeqNum_.load();
}
