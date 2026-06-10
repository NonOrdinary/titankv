#include "memtable.hpp"
#include <mutex>

MemTable::MemTable()
    : data_(std::make_unique<SkipList>()), sizeBytes_(0) {}

void MemTable::Put(const std::vector<uint8_t>& userKey, const std::vector<uint8_t>& value, uint64_t seqNum) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    std::vector<uint8_t> internalKey = EncodeInternalKey(userKey, seqNum, TypePut);
    data_->Insert(internalKey, value);
    sizeBytes_ += internalKey.size() + value.size() + skipListNodeOverhead;
}

bool MemTable::Get(const std::vector<uint8_t>& userKey, uint64_t targetSeqNum, std::vector<uint8_t>& val, bool& isDeleted) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<uint8_t> searchKey = EncodeInternalKey(userKey, targetSeqNum, TypePut);
    uint8_t keyType = 0;
    bool found = data_->Search(searchKey, val, keyType);
    if (!found) {
        return false;
    }
    if (keyType == TypeDelete) {
        isDeleted = true;
        return true;
    }
    isDeleted = false;
    return true;
}

void MemTable::Delete(const std::vector<uint8_t>& userKey, uint64_t seqNum) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    std::vector<uint8_t> internalKey = EncodeInternalKey(userKey, seqNum, TypeDelete);
    data_->Insert(internalKey, {});
    sizeBytes_ += internalKey.size() + skipListNodeOverhead;
}

size_t MemTable::ApproximateSize() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return sizeBytes_;
}
