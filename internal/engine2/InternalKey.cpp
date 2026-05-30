    #include "InternalKey.hpp"

std::string EncodeInternalKey(const std::string& userKey, uint64_t seqNum, uint8_t keyType) {
    std::string buf = userKey;
    buf.resize(userKey.size() + internalKeySuffixLen);
    
    size_t offset = userKey.size();
    
    buf[offset]     = (seqNum >> 56) & 0xFF;
    buf[offset + 1] = (seqNum >> 48) & 0xFF;
    buf[offset + 2] = (seqNum >> 40) & 0xFF;
    buf[offset + 3] = (seqNum >> 32) & 0xFF;
    buf[offset + 4] = (seqNum >> 24) & 0xFF;
    buf[offset + 5] = (seqNum >> 16) & 0xFF;
    buf[offset + 6] = (seqNum >> 8) & 0xFF;
    buf[offset + 7] = seqNum & 0xFF;
    
    buf[offset + 8] = keyType;
    
    return buf;
}

std::tuple<std::string, uint64_t, uint8_t> ParseInternalKey(const std::string& internalKey) {
    if (internalKey.size() < internalKeySuffixLen) {
        return {"", 0, 0};
    }

    size_t suffixStart = internalKey.size() - internalKeySuffixLen;
    std::string userKey = internalKey.substr(0, suffixStart);

    uint64_t seqNum = 0;
    for (int i = 0; i < 8; i++) {
        seqNum = (seqNum << 8) | static_cast<uint8_t>(internalKey[suffixStart + i]);
    }

    uint8_t keyType = static_cast<uint8_t>(internalKey.back());

    return {userKey, seqNum, keyType};
}

int CompareInternalKeys(const std::string& a, const std::string& b) {
    if (a.size() < internalKeySuffixLen || b.size() < internalKeySuffixLen) {
        return 0;
    }

    size_t userKeyLenA = a.size() - internalKeySuffixLen;
    size_t userKeyLenB = b.size() - internalKeySuffixLen;

    std::string userKeyA = a.substr(0, userKeyLenA);
    std::string userKeyB = b.substr(0, userKeyLenB);

    int cmp = userKeyA.compare(userKeyB);
    if (cmp != 0) {
        return cmp < 0 ? -1 : 1;
    }

    uint64_t seqNumA = 0;
    for (int i = 0; i < 8; i++) {
        seqNumA = (seqNumA << 8) | static_cast<uint8_t>(a[userKeyLenA + i]);
    }

    uint64_t seqNumB = 0;
    for (int i = 0; i < 8; i++) {
        seqNumB = (seqNumB << 8) | static_cast<uint8_t>(b[userKeyLenB + i]);
    }

    if (seqNumA > seqNumB) {
        return -1;
    } else if (seqNumA < seqNumB) {
        return 1;
    }

    uint8_t typeA = static_cast<uint8_t>(a.back());
    uint8_t typeB = static_cast<uint8_t>(b.back());

    if (typeA > typeB) {
        return -1;
    } else if (typeA < typeB) {
        return 1;
    }

    return 0;
}