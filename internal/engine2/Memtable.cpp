#include "MemTable.hpp"

MemTable::MemTable() : data(new SkipList()), sizeBytes(0) {}

MemTable::~MemTable() {
    delete data;
}

void MemTable::Put(const std::string& userKey, const std::string& value, uint64_t seqNum) {
    std::unique_lock<std::shared_mutex> lock(mu);
    
    std::string internalKey = EncodeInternalKey(userKey, seqNum, TypePut);
    data->Insert(internalKey, value);
    
    sizeBytes += internalKey.size() + value.size() + skipListNodeOverhead;
}

std::tuple<std::string, bool, bool> MemTable::Get(const std::string& userKey, uint64_t targetSeqNum) {
    std::shared_lock<std::shared_mutex> lock(mu);
    
    std::string searchKey = EncodeInternalKey(userKey, targetSeqNum, TypePut);
    auto [val, keyType, found] = data->Search(searchKey);

    if (!found) {
        return {"", false, false};
    }

    if (keyType == TypeDelete) {
        return {"", true, true};
    }

    return {val, false, true};
}

void MemTable::Delete(const std::string& userKey, uint64_t seqNum) {
    std::unique_lock<std::shared_mutex> lock(mu);
    
    std::string internalKey = EncodeInternalKey(userKey, seqNum, TypeDelete);
    data->Insert(internalKey, "");
    
    sizeBytes += internalKey.size() + skipListNodeOverhead;
}

void MemTable::Iterate(std::function<void(const std::string&, const std::string&)> cb) {
    std::shared_lock<std::shared_mutex> lock(mu);
    data->Iterate(cb);
}

size_t MemTable::ApproximateSize() {
    std::shared_lock<std::shared_mutex> lock(mu);
    return sizeBytes;
}