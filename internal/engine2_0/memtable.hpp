#pragma once

#include <vector>
#include <cstdint>
#include <shared_mutex>
#include <memory>
#include "skiplist.hpp"

constexpr size_t skipListNodeOverhead = 130;

class MemTable {
public:
    MemTable();
    ~MemTable() = default;

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Put(const std::vector<uint8_t>& userKey, const std::vector<uint8_t>& value, uint64_t seqNum);
    bool Get(const std::vector<uint8_t>& userKey, uint64_t targetSeqNum, std::vector<uint8_t>& val, bool& isDeleted) const;
    void Delete(const std::vector<uint8_t>& userKey, uint64_t seqNum);

    size_t ApproximateSize() const;

    template <typename Callback>
    void Iterate(Callback cb) const {
        std::shared_lock<std::shared_mutex> lock(mu_);
        data_->Iterate(cb);
    }

private:
    mutable std::shared_mutex mu_;
    std::unique_ptr<SkipList> data_;
    size_t sizeBytes_;
};
