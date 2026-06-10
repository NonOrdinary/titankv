#include "sstable_reader.hpp"
#include "internal_key.hpp"
#include <algorithm>
#include <stdexcept>
#include <cstring>

static uint32_t Uint32LE(const uint8_t* buf) {
    return static_cast<uint32_t>(buf[0]) |
           (static_cast<uint32_t>(buf[1]) << 8) |
           (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) << 24);
}

SSTableReader::SSTableReader(const std::string& path)
    : path_(path), indexStartOffset_(0), bloomStartOffset_(0) {}

SSTableReader::~SSTableReader() {
    Close();
}

std::shared_ptr<SSTableReader> SSTableReader::Open(const std::string& path) {
    auto reader = std::shared_ptr<SSTableReader>(new SSTableReader(path));
    reader->file_.open(path, std::ios::in | std::ios::binary);
    if (!reader->file_.is_open()) {
        return nullptr;
    }

    // 1. Read Footer (16 bytes from end)
    reader->file_.seekg(-16, std::ios::end);
    uint8_t footerBuf[16];
    reader->file_.read(reinterpret_cast<char*>(footerBuf), 16);
    if (reader->file_.gcount() != 16) {
        return nullptr;
    }

    reader->bloomStartOffset_ = Uint32LE(footerBuf);
    reader->indexStartOffset_ = Uint32LE(footerBuf + 4);
    uint32_t metaStartOffset = Uint32LE(footerBuf + 8);
    uint32_t magicNumber = Uint32LE(footerBuf + 12);

    if (magicNumber != 0xABCD1234U) {
        return nullptr;
    }

    // 2. Load Bloom Filter
    uint32_t bloomSize = reader->indexStartOffset_ - reader->bloomStartOffset_;
    std::vector<uint8_t> bloomBytes(bloomSize);
    reader->file_.seekg(reader->bloomStartOffset_, std::ios::beg);
    reader->file_.read(reinterpret_cast<char*>(bloomBytes.data()), bloomSize);
    if (reader->file_.gcount() != bloomSize) {
        return nullptr;
    }
    reader->bloom_ = std::make_unique<BloomFilter>(std::move(bloomBytes), 3);

    // 3. Load Sparse Index
    reader->file_.seekg(reader->indexStartOffset_, std::ios::beg);
    uint8_t countBuf[4];
    reader->file_.read(reinterpret_cast<char*>(countBuf), 4);
    if (reader->file_.gcount() != 4) {
        return nullptr;
    }
    uint32_t numEntries = Uint32LE(countBuf);

    reader->index_.resize(numEntries);
    for (uint32_t i = 0; i < numEntries; i++) {
        reader->file_.read(reinterpret_cast<char*>(countBuf), 4);
        if (reader->file_.gcount() != 4) {
            return nullptr;
        }
        uint32_t keyLen = Uint32LE(countBuf);

        std::vector<uint8_t> keyBuf(keyLen);
        reader->file_.read(reinterpret_cast<char*>(keyBuf.data()), keyLen);
        if (reader->file_.gcount() != keyLen) {
            return nullptr;
        }

        reader->file_.read(reinterpret_cast<char*>(countBuf), 4);
        if (reader->file_.gcount() != 4) {
            return nullptr;
        }
        uint32_t offset = Uint32LE(countBuf);

        reader->index_[i] = IndexEntry{std::move(keyBuf), offset};
    }

    // 4. Load Metadata
    reader->file_.seekg(metaStartOffset, std::ios::beg);
    uint8_t lenBuf[4];
    
    reader->file_.read(reinterpret_cast<char*>(lenBuf), 4);
    if (reader->file_.gcount() != 4) {
        return nullptr;
    }
    uint32_t minLen = Uint32LE(lenBuf);
    std::vector<char> minBuf(minLen);
    reader->file_.read(minBuf.data(), minLen);
    if (reader->file_.gcount() != minLen) {
        return nullptr;
    }
    reader->minKey_ = std::string(minBuf.data(), minLen);

    reader->file_.read(reinterpret_cast<char*>(lenBuf), 4);
    if (reader->file_.gcount() != 4) {
        return nullptr;
    }
    uint32_t maxLen = Uint32LE(lenBuf);
    std::vector<char> maxBuf(maxLen);
    reader->file_.read(maxBuf.data(), maxLen);
    if (reader->file_.gcount() != maxLen) {
        return nullptr;
    }
    reader->maxKey_ = std::string(maxBuf.data(), maxLen);

    return reader;
}

bool SSTableReader::Get(const std::vector<uint8_t>& userKey, uint64_t targetSeqNum, std::vector<uint8_t>& val, bool& isDeleted) const {
    if (!bloom_->MightContain(std::string(userKey.begin(), userKey.end()))) {
        return false;
    }

    if (index_.empty()) {
        return false;
    }

    // Binary Search
    auto it = std::lower_bound(index_.begin(), index_.end(), userKey, [](const IndexEntry& entry, const std::vector<uint8_t>& key) {
        return entry.Key < key;
    });
    size_t idx = std::distance(index_.begin(), it);

    if (idx > 0 && (idx == index_.size() || index_[idx].Key > userKey)) {
        idx--;
    }

    uint32_t blockStart = index_[idx].Offset;
    uint32_t blockEnd = 0;
    if (idx + 1 < index_.size()) {
        blockEnd = index_[idx + 1].Offset;
    } else {
        blockEnd = bloomStartOffset_;
    }

    uint32_t bytesToRead = blockEnd - blockStart;
    std::vector<uint8_t> blockBuf(bytesToRead);

    {
        std::lock_guard<std::mutex> lock(file_mutex_);
        if (!file_.is_open()) return false;
        file_.seekg(blockStart, std::ios::beg);
        file_.read(reinterpret_cast<char*>(blockBuf.data()), bytesToRead);
        if (file_.gcount() != bytesToRead) {
            return false;
        }
    }

    size_t currentOffset = 0;
    while (currentOffset < bytesToRead) {
        if (currentOffset + 4 > bytesToRead) break;
        uint32_t intKeyLen = Uint32LE(blockBuf.data() + currentOffset);
        currentOffset += 4;

        if (currentOffset + intKeyLen > bytesToRead) break;
        std::vector<uint8_t> internalKey(blockBuf.begin() + currentOffset, blockBuf.begin() + currentOffset + intKeyLen);
        currentOffset += intKeyLen;

        if (currentOffset + 4 > bytesToRead) break;
        uint32_t valLen = Uint32LE(blockBuf.data() + currentOffset);
        currentOffset += 4;

        if (currentOffset + valLen > bytesToRead) break;
        std::vector<uint8_t> valBytes(blockBuf.begin() + currentOffset, blockBuf.begin() + currentOffset + valLen);
        
        std::vector<uint8_t> currUserKey;
        uint64_t currSeqNum = 0;
        uint8_t currType = 0;
        if (ParseInternalKey(internalKey, currUserKey, currSeqNum, currType)) {
            if (currUserKey == userKey && currSeqNum <= targetSeqNum) {
                if (currType == TypeDelete) {
                    isDeleted = true;
                    return true;
                }
                val = std::move(valBytes);
                isDeleted = false;
                return true;
            }
            if (currUserKey > userKey) {
                break;
            }
        }

        currentOffset += valLen;
    }

    return false;
}

void SSTableReader::Close() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (file_.is_open()) {
        file_.close();
    }
}

std::string SSTableReader::GetPath() const {
    return path_;
}

std::string SSTableReader::GetMinKey() const {
    return minKey_;
}

std::string SSTableReader::GetMaxKey() const {
    return maxKey_;
}

uint32_t SSTableReader::GetBloomStartOffset() const {
    return bloomStartOffset_;
}
