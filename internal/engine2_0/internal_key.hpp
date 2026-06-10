#pragma once

#include <vector>
#include <cstdint>
#include <cstddef>

constexpr uint8_t TypeDelete = 0;
constexpr uint8_t TypePut = 1;
constexpr size_t internalKeySuffixLen = 9;

std::vector<uint8_t> EncodeInternalKey(const std::vector<uint8_t>& userKey, uint64_t seqNum, uint8_t keyType);

bool ParseInternalKey(const std::vector<uint8_t>& internalKey, std::vector<uint8_t>& userKey, uint64_t& seqNum, uint8_t& keyType);

int CompareInternalKeys(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b);
