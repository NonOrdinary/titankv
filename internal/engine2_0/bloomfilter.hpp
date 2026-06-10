#pragma once

#include <vector>
#include <string>
#include <cstdint>

class BloomFilter {
public:
    BloomFilter(size_t sizeInBytes, int numHashes);
    BloomFilter(std::vector<uint8_t> data, int numHashes);

    void Add(const std::string& key);
    bool MightContain(const std::string& key) const;
    const std::vector<uint8_t>& Bytes() const;

private:
    std::vector<uint8_t> bitset_;
    int numHashes_;
};
