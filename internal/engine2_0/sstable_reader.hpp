#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <mutex>
#include <memory>
#include "bloomfilter.hpp"
#include "sstable_builder.hpp"

class SSTableReader {
public:
    static std::shared_ptr<SSTableReader> Open(const std::string& path);
    ~SSTableReader();

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;

    bool Get(const std::vector<uint8_t>& userKey, uint64_t targetSeqNum, std::vector<uint8_t>& val, bool& isDeleted) const;
    void Close();

    std::string GetPath() const;
    std::string GetMinKey() const;
    std::string GetMaxKey() const;
    uint32_t GetBloomStartOffset() const;

private:
    explicit SSTableReader(const std::string& path);

    std::string path_;
    mutable std::ifstream file_;
    mutable std::mutex file_mutex_;

    std::vector<IndexEntry> index_;
    uint32_t indexStartOffset_;
    uint32_t bloomStartOffset_;
    std::unique_ptr<BloomFilter> bloom_;
    std::string minKey_;
    std::string maxKey_;
};
