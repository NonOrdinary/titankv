#include "internal_key.hpp"
#include <algorithm>
#include <cstring>

static void PutUint64BE(uint8_t* buf, uint64_t val) {
    buf[0] = static_cast<uint8_t>(val >> 56);
    buf[1] = static_cast<uint8_t>(val >> 48);
    buf[2] = static_cast<uint8_t>(val >> 40);
    buf[3] = static_cast<uint8_t>(val >> 32);
    buf[4] = static_cast<uint8_t>(val >> 24);
    buf[5] = static_cast<uint8_t>(val >> 16);
    buf[6] = static_cast<uint8_t>(val >> 8);
    buf[7] = static_cast<uint8_t>(val);
}

static uint64_t Uint64BE(const uint8_t* buf) {
    return (static_cast<uint64_t>(buf[0]) << 56) |
           (static_cast<uint64_t>(buf[1]) << 48) |
           (static_cast<uint64_t>(buf[2]) << 40) |
           (static_cast<uint64_t>(buf[3]) << 32) |
           (static_cast<uint64_t>(buf[4]) << 24) |
           (static_cast<uint64_t>(buf[5]) << 16) |
           (static_cast<uint64_t>(buf[6]) << 8) |
           static_cast<uint64_t>(buf[7]);
}

std::vector<uint8_t> EncodeInternalKey(const std::vector<uint8_t>& userKey, uint64_t seqNum, uint8_t keyType) {
    size_t size = userKey.size() + internalKeySuffixLen;
    std::vector<uint8_t> buf(size);
    if (!userKey.empty()) {
        std::copy(userKey.begin(), userKey.end(), buf.begin());
    }
    PutUint64BE(buf.data() + userKey.size(), seqNum);
    buf[size - 1] = keyType;
    return buf;
}

bool ParseInternalKey(const std::vector<uint8_t>& internalKey, std::vector<uint8_t>& userKey, uint64_t& seqNum, uint8_t& keyType) {
    if (internalKey.size() < internalKeySuffixLen) {
        return false;
    }
    size_t suffixStart = internalKey.size() - internalKeySuffixLen;
    userKey.assign(internalKey.begin(), internalKey.begin() + suffixStart);
    seqNum = Uint64BE(internalKey.data() + suffixStart);
    keyType = internalKey.back();
    return true;
}

int CompareInternalKeys(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b) {
    if (a.size() < internalKeySuffixLen || b.size() < internalKeySuffixLen) {
        return 0;
    }

    size_t userKeyLenA = a.size() - internalKeySuffixLen;
    size_t userKeyLenB = b.size() - internalKeySuffixLen;

    size_t minLen = std::min(userKeyLenA, userKeyLenB);
    for (size_t i = 0; i < minLen; ++i) {
        if (a[i] < b[i]) return -1;
        if (a[i] > b[i]) return 1;
    }
    if (userKeyLenA < userKeyLenB) return -1;
    if (userKeyLenA > userKeyLenB) return 1;

    uint64_t seqNumA = Uint64BE(a.data() + userKeyLenA);
    uint64_t seqNumB = Uint64BE(b.data() + userKeyLenB);

    if (seqNumA > seqNumB) {
        return -1;
    } else if (seqNumA < seqNumB) {
        return 1;
    }

    uint8_t typeA = a.back();
    uint8_t typeB = b.back();

    if (typeA > typeB) {
        return -1;
    } else if (typeA < typeB) {
        return 1;
    }

    return 0;
}
