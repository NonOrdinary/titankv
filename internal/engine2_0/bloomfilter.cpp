#include "bloomfilter.hpp"

static uint32_t fnv32a(const std::string& key) {
    uint32_t hash = 2166136261U;
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619U;
    }
    return hash;
}

BloomFilter::BloomFilter(size_t sizeInBytes, int numHashes)
    : bitset_(sizeInBytes, 0), numHashes_(numHashes) {}

BloomFilter::BloomFilter(std::vector<uint8_t> data, int numHashes)
    : bitset_(std::move(data)), numHashes_(numHashes) {}

void BloomFilter::Add(const std::string& key) {
    uint32_t hash = fnv32a(key);
    uint32_t numBits = static_cast<uint32_t>(bitset_.size() * 8);
    if (numBits == 0) return;

    for (int i = 0; i < numHashes_; i++) {
        uint32_t combinedHash = hash + static_cast<uint32_t>(i) * 0x9e3779b9U;
        uint32_t bitIdx = combinedHash % numBits;
        uint32_t byteIdx = bitIdx / 8;
        uint32_t bitOffset = bitIdx % 8;
        bitset_[byteIdx] |= (1 << bitOffset);
    }
}

bool BloomFilter::MightContain(const std::string& key) const {
    if (bitset_.empty()) {
        return true;
    }

    uint32_t hash = fnv32a(key);
    uint32_t numBits = static_cast<uint32_t>(bitset_.size() * 8);
    if (numBits == 0) return true;

    for (int i = 0; i < numHashes_; i++) {
        uint32_t combinedHash = hash + static_cast<uint32_t>(i) * 0x9e3779b9U;
        uint32_t bitIdx = combinedHash % numBits;
        uint32_t byteIdx = bitIdx / 8;
        uint32_t bitOffset = bitIdx % 8;

        if ((bitset_[byteIdx] & (1 << bitOffset)) == 0) {
            return false;
        }
    }
    return true;
}

const std::vector<uint8_t>& BloomFilter::Bytes() const {
    return bitset_;
}
