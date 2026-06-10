#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <cstdint>

struct IteratorKV {
    std::vector<uint8_t> InternalKey;
    std::vector<uint8_t> Value;
};

class SSTableIterator {
public:
    SSTableIterator(const std::string& path, uint32_t endPos);
    ~SSTableIterator();

    SSTableIterator(const SSTableIterator&) = delete;
    SSTableIterator& operator=(const SSTableIterator&) = delete;

    bool Next(IteratorKV& kv);
    void Close();

private:
    std::ifstream file_;
    uint32_t endPos_;
    uint32_t curr_;
};
