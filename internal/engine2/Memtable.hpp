#pragma once

#include "SkipList.hpp"
#include "InternalKey.hpp"
#include <string>
#include <mutex>

#include <shared_mutex>
#include <functional>
#include <tuple>
#include <cstdint>

const size_t skipListNodeOverhead = 130;

class MemTable {
private:
    std::shared_mutex mu;
    SkipList* data;
    size_t sizeBytes;

public:
    MemTable();
    ~MemTable();

    void Put(const std::string& userKey, const std::string& value, uint64_t seqNum);
    std::tuple<std::string, bool, bool> Get(const std::string& userKey, uint64_t targetSeqNum);
    void Delete(const std::string& userKey, uint64_t seqNum);
    void Iterate(std::function<void(const std::string&, const std::string&)> cb);
    size_t ApproximateSize();
};