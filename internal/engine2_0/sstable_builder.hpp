#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <memory>
#include "bloomfilter.hpp"

constexpr uint32_t blockSize = 4096;

struct IndexEntry {
    std::vector<uint8_t> Key;
    uint32_t Offset;
};

class SSTableBuilder {
public:
    explicit SSTableBuilder(const std::string& path);
    ~SSTableBuilder();

    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;

    bool Add(const std::vector<uint8_t>& internalKey, const std::vector<uint8_t>& value);
    bool Finish();

private:
    std::ofstream file_;
    uint32_t offset_;
    uint32_t blockBytesWritten_;
    std::vector<IndexEntry> index_;
    std::vector<uint8_t> minKey_;
    std::vector<uint8_t> maxKey_;
    std::unique_ptr<BloomFilter> bloom_;
    std::vector<uint8_t> scratchBuf_;
};
