#include "DB.hpp"
#include <chrono>
#include <limits>

DB::DB(const std::string& dbDir) 
    : dir(dbDir), activeMemTable(nullptr), immutableMemTable(nullptr), 
      activeWAL(nullptr), manifest(nullptr), isCompacting(false), 
      maxMemtableSize(4 * 1024 * 1024), nextSeqNum(0), isClosed(false) {}

DB::~DB() {
    Close();
}

DB* DB::Open(const std::string& dir) {
    std::filesystem::create_directories(dir);

    std::string manifestPath = (std::filesystem::path(dir) / "MANIFEST.log").string();
    std::vector<ManifestRecord> activeRecords; 

    std::vector<SSTableReader*> sstables;
    for (const auto& rec : activeRecords) {
        SSTableReader* reader = new SSTableReader();
        sstables.push_back(reader);
    }

    Manifest* manifest = new Manifest(manifestPath);
    std::string walPath = (std::filesystem::path(dir) / "active.wal").string();
    WAL* wal = new WAL();

    MemTable* mt = new MemTable();
    uint64_t maxWalSeq = wal->Recover(mt);

    DB* db = new DB(dir);
    db->activeMemTable = mt;
    db->activeWAL = wal;
    db->manifest = manifest;
    db->sstables = sstables;

    uint64_t baseSeq = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    if (maxWalSeq >= baseSeq) {
        baseSeq = maxWalSeq + 1;
    }
    db->nextSeqNum.store(baseSeq);

    db->flushThread = std::thread(&DB::flushWorker, db);

    return db;
}

bool DB::Put(const std::string& key, const std::string& value) {
    std::unique_lock<std::shared_mutex> lock(mu);

    while (immutableMemTable != nullptr) {
        mu.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        mu.lock();
    }

    uint64_t seqNum = ++nextSeqNum;
    std::string internalKey = EncodeInternalKey(key, seqNum, TypePut);

    if (!activeWAL->WriteRecord(internalKey, value)) {
        return false;
    }

    activeMemTable->Put(key, value, seqNum);

    if (activeMemTable->ApproximateSize() >= maxMemtableSize) {
        triggerFlush();
    } else {
        mu.unlock();
    }

    return true;
}

bool DB::Delete(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(mu);

    while (immutableMemTable != nullptr) {
        mu.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        mu.lock();
    }

    uint64_t seqNum = ++nextSeqNum;
    std::string internalKey = EncodeInternalKey(key, seqNum, TypeDelete);

    if (!activeWAL->WriteRecord(internalKey, "")) {
        return false;
    }

    activeMemTable->Delete(key, seqNum);

    if (activeMemTable->ApproximateSize() >= maxMemtableSize) {
        triggerFlush();
    } else {
        mu.unlock();
    }

    return true;
}

std::tuple<std::string, bool, bool> DB::Get(const std::string& key) {
    return GetAt(key, std::numeric_limits<uint64_t>::max());
}

std::tuple<std::string, bool, bool> DB::GetAt(const std::string& key, uint64_t targetSeqNum) {
    std::shared_lock<std::shared_mutex> lock(mu);

    auto [val, isDeleted, exists] = activeMemTable->Get(key, targetSeqNum);
    if (exists) {
        if (isDeleted) {
            return {"", false, true};
        }
        return {val, true, true};
    }

    if (immutableMemTable != nullptr) {
        std::tie(val, isDeleted, exists) = immutableMemTable->Get(key, targetSeqNum);
        if (exists) {
            if (isDeleted) {
                return {"", false, true};
            }
            return {val, true, true};
        }
    }

    for (int i = static_cast<int>(sstables.size()) - 1; i >= 0; i--) {
        SSTableReader* sst = sstables[i];

        if (key < sst->MinKey || key > sst->MaxKey) {
            continue;
        }

        auto [sstVal, sstIsDeleted, sstFound, sstSuccess] = sst->Get(key, targetSeqNum);
        if (!sstSuccess) {
            return {"", false, false};
        }
        if (sstFound) {
            if (sstIsDeleted) {
                return {"", false, true};
            }
            return {sstVal, true, true};
        }
    }

    return {"", false, true};
}

void DB::triggerFlush() {
    MemTable* frozenMemTable = activeMemTable;
    WAL* frozenWAL = activeWAL;

    immutableMemTable = frozenMemTable;
    activeMemTable = new MemTable();

    std::string newWALPath = (std::filesystem::path(dir) / ("wal_" + std::to_string(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count()) + ".log")).string();
    
    activeWAL = new WAL();

    mu.unlock();

    FlushTask* task = new FlushTask{frozenMemTable, frozenWAL};
    {
        std::lock_guard<std::mutex> qLock(queueMu);
        flushQueue.push(task);
    }
    queueCv.notify_one();
}

void DB::flushWorker() {
    while (true) {
        FlushTask* task = nullptr;
        {
            std::unique_lock<std::mutex> qLock(queueMu);
            queueCv.wait(qLock, [this] { return !flushQueue.empty() || isClosed; });
            
            if (flushQueue.empty() && isClosed) {
                break;
            }
            
            task = flushQueue.front();
            flushQueue.pop();
        }

        if (task) {
            std::string sstPath = (std::filesystem::path(dir) / ("sst_" + std::to_string(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()) + ".sst")).string();
            
            SSTableBuilder* builder = new SSTableBuilder(sstPath);
            task->mt->Iterate([builder](const std::string& internalKey, const std::string& value) {
                builder->Add(internalKey, value);
            });
            builder->Finish();
            delete builder;

            SSTableReader* reader = new SSTableReader();

            mu.lock();

            manifest->Append(ManifestRecord{
                "ADD",
                sstPath,
                reader->MinKey,
                reader->MaxKey
            });

            sstables.push_back(reader);
            delete task->mt;
            immutableMemTable = nullptr;

            size_t numTables = sstables.size();
            bool isComp = isCompacting;

            if (numTables >= 4 && !isComp) {
                isCompacting = true;
                std::thread(&DB::Compact, this).detach();
            }

            mu.unlock();

            task->wal->Close();
            std::filesystem::remove(task->wal->GetPath());
            delete task->wal;
            delete task;
        }
    }
}

void DB::Compact() {
    // Will be implemented during Compaction Phase
}

void DB::Close() {
    {
        std::lock_guard<std::mutex> qLock(queueMu);
        if (isClosed) return;
        isClosed = true;
    }
    queueCv.notify_one();
    
    if (flushThread.joinable()) {
        flushThread.join();
    }

    std::unique_lock<std::shared_mutex> lock(mu);

    if (activeWAL) {
        activeWAL->Close();
        delete activeWAL;
        activeWAL = nullptr;
    }

    if (manifest) {
        manifest->Close();
        delete manifest;
        manifest = nullptr;
    }

    for (auto sst : sstables) {
        sst->Close();
        delete sst;
    }
    sstables.clear();

    delete activeMemTable;
    delete immutableMemTable;
    activeMemTable = nullptr;
    immutableMemTable = nullptr;
}