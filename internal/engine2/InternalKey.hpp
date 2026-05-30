#pragma once

#include <string>
#include <cstdint>
#include <tuple>

const uint8_t TypeDelete = 0;
const uint8_t TypePut = 1;
const size_t internalKeySuffixLen = 9;

std::string EncodeInternalKey(const std::string& userKey, uint64_t seqNum, uint8_t keyType);
std::tuple<std::string, uint64_t, uint8_t> ParseInternalKey(const std::string& internalKey);
int CompareInternalKeys(const std::string& a, const std::string& b);