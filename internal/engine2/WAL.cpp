#include "WAL.hpp"
#include "InternalKey.hpp"
#include <filesystem>
#include <vector>

WAL::WAL(const std::string& path) : filePath(path) {
    file.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file.is_open()) {
        file.clear();
        file.open(path, std::ios::out | std::ios::binary);
        file.close();
        file.open(path, std::ios::in | std::ios::out | std::ios::binary);
    }
}

WAL::~WAL() {
    Close();
}

void WAL::putUint32LE(char* buf, uint32_t val) {
    buf[0] = val & 0xFF;
    buf[1] = (val >> 8) & 0xFF;
    buf[2] = (val >> 16) & 0xFF;
    buf[3] = (val >> 24) & 0xFF;
}

uint32_t WAL::getUint32LE(const char* buf) {
    return static_cast<uint32_t>(static_cast<uint8_t>(buf[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(buf[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(buf[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(buf[3])) << 24);
}

uint32_t WAL::computeCRC32(const char* data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc ^= static_cast<uint8_t>(data[i]);
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc = crc >> 1;
            }
        }
    }
    return ~crc;
}

bool WAL::WriteRecord(const std::string& internalKey, const std::string& value) {
    size_t payloadSize = 4 + internalKey.size() + 4 + value.size();
    size_t totalSize = 4 + payloadSize;

    std::vector<char> buf(totalSize);
    size_t offset = 4;

    putUint32LE(&buf[offset], static_cast<uint32_t>(internalKey.size()));
    offset += 4;

    std::memcpy(&buf[offset], internalKey.data(), internalKey.size());
    offset += internalKey.size();

    putUint32LE(&buf[offset], static_cast<uint32_t>(value.size()));
    offset += 4;

    std::memcpy(&buf[offset], value.data(), value.size());

    uint32_t checksum = computeCRC32(&buf[4], payloadSize);
    putUint32LE(&buf[0], checksum);

    std::lock_guard<std::mutex> lock(mu);
    if (!file.is_open()) return false;
    
    file.write(buf.data(), totalSize);
    return file.good();
}

uint64_t WAL::Recover(MemTable* mt) {
    std::lock_guard<std::mutex> lock(mu);
    if (!file.is_open()) return 0;

    file.seekg(0, std::ios::beg);

    uint64_t maxSeqNum = 0;
    uint64_t validOffset = 0;

    char lenBuf[4];
    char checksumBuf[4];

    while (true) {
        file.read(checksumBuf, 4);
        if (file.gcount() < 4) break;
        uint32_t expectedChecksum = getUint32LE(checksumBuf);

        file.read(lenBuf, 4);
        if (file.gcount() < 4) break;
        uint32_t keyLen = getUint32LE(lenBuf);

        std::string internalKey(keyLen, '\0');
        file.read(&internalKey[0], keyLen);
        if (file.gcount() < keyLen) break;

        file.read(lenBuf, 4);
        if (file.gcount() < 4) break;
        uint32_t valLen = getUint32LE(lenBuf);

        std::string value(valLen, '\0');
        if (valLen > 0) {
            file.read(&value[0], valLen);
            if (file.gcount() < valLen) break;
        }

        std::vector<char> verifyBuf(4 + internalKey.size() + 4 + value.size());
        putUint32LE(&verifyBuf[0], keyLen);
        std::memcpy(&verifyBuf[4], internalKey.data(), internalKey.size());
        putUint32LE(&verifyBuf[4 + internalKey.size()], valLen);
        if (valLen > 0) {
            std::memcpy(&verifyBuf[4 + internalKey.size() + 4], value.data(), value.size());
        }

        if (computeCRC32(verifyBuf.data(), verifyBuf.size()) != expectedChecksum) {
            break;
        }

        auto [uKey, seq, kType] = ParseInternalKey(internalKey);
        if (seq > maxSeqNum) {
            maxSeqNum = seq;
        }

        if (kType == TypeDelete) {
            mt->Delete(uKey, seq);
        } else {
            mt->Put(uKey, value, seq);
        }

        validOffset += 4 + 4 + keyLen + 4 + valLen;
    }

    file.close();
    std::filesystem::resize_file(filePath, validOffset);
    
    file.open(filePath, std::ios::in | std::ios::out | std::ios::binary);
    file.seekp(validOffset, std::ios::beg);

    return maxSeqNum;
}

void WAL::Close() {
    std::lock_guard<std::mutex> lock(mu);
    if (file.is_open()) {
        file.close();
    }
}

std::string WAL::GetPath() const {
    return filePath;
}