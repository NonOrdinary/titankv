#include "wal.hpp"
#include "memtable.hpp"
#include <filesystem>
#include <algorithm>

static void PutUint32LE(uint8_t* buf, uint32_t val) {
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
    buf[2] = static_cast<uint8_t>(val >> 16);
    buf[3] = static_cast<uint8_t>(val >> 24);
}

static uint32_t Uint32LE(const uint8_t* buf) {
    return static_cast<uint32_t>(buf[0]) |
           (static_cast<uint32_t>(buf[1]) << 8) |
           (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) << 24);
}

static uint32_t crc32_table[256];
static bool crc32_table_initialized = false;

static void init_crc32_table() {
    if (crc32_table_initialized) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (uint32_t j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xedb88320U;
            } else {
                crc >>= 1;
            }
        }
        crc32_table[i] = crc;
    }
    crc32_table_initialized = true;
}

static uint32_t crc32_ieee(const uint8_t* data, size_t len) {
    init_crc32_table();
    uint32_t crc = 0xffffffffU;
    for (size_t i = 0; i < len; i++) {
        uint8_t lookup = (crc ^ data[i]) & 0xff;
        crc = (crc >> 8) ^ crc32_table[lookup];
    }
    return crc ^ 0xffffffffU;
}

WAL::WAL(const std::string& path) : path_(path) {}

WAL::~WAL() {
    Close();
}

std::unique_ptr<WAL> WAL::Open(const std::string& path) {
    auto wal = std::unique_ptr<WAL>(new WAL(path));
    
    // Create file if it doesn't exist
    {
        std::ofstream create(path, std::ios::binary | std::ios::app);
    }
    
    wal->file_.open(path, std::ios::in | std::ios::out | std::ios::binary);
    if (!wal->file_.is_open()) {
        return nullptr;
    }
    return wal;
}

bool WAL::WriteRecord(const std::vector<uint8_t>& internalKey, const std::vector<uint8_t>& value) {
    std::lock_guard<std::mutex> lock(mu_);
    size_t payloadSize = 4 + internalKey.size() + 4 + value.size();
    size_t totalSize = 4 + payloadSize;

    if (write_buf_.size() < totalSize) {
        write_buf_.resize(totalSize);
    }

    size_t offset = 4;
    PutUint32LE(write_buf_.data() + offset, static_cast<uint32_t>(internalKey.size()));
    offset += 4;

    std::copy(internalKey.begin(), internalKey.end(), write_buf_.data() + offset);
    offset += internalKey.size();

    PutUint32LE(write_buf_.data() + offset, static_cast<uint32_t>(value.size()));
    offset += 4;

    if (!value.empty()) {
        std::copy(value.begin(), value.end(), write_buf_.data() + offset);
    }

    uint32_t checksum = crc32_ieee(write_buf_.data() + 4, payloadSize);
    PutUint32LE(write_buf_.data(), checksum);

    if (!file_.write(reinterpret_cast<const char*>(write_buf_.data()), totalSize)) {
        return false;
    }
    file_.flush();
    return true;
}

uint64_t WAL::Recover(MemTable* mt) {
    std::lock_guard<std::mutex> lock(mu_);
    
    file_.seekg(0, std::ios::beg);
    
    uint64_t maxSeqNum = 0;
    int64_t validOffset = 0;
    
    std::vector<uint8_t> headerBuf(4);
    std::vector<uint8_t> lenBuf(4);
    std::vector<uint8_t> scratchBuf;
    
    while (true) {
        file_.read(reinterpret_cast<char*>(headerBuf.data()), 4);
        if (file_.gcount() != 4) {
            break;
        }
        uint32_t expectedChecksum = Uint32LE(headerBuf.data());

        file_.read(reinterpret_cast<char*>(lenBuf.data()), 4);
        if (file_.gcount() != 4) {
            break;
        }
        uint32_t keyLen = Uint32LE(lenBuf.data());

        if (scratchBuf.size() < keyLen) {
            scratchBuf.resize(keyLen);
        }
        file_.read(reinterpret_cast<char*>(scratchBuf.data()), keyLen);
        if (file_.gcount() != keyLen) {
            break;
        }
        std::vector<uint8_t> finalIK(scratchBuf.begin(), scratchBuf.begin() + keyLen);

        file_.read(reinterpret_cast<char*>(lenBuf.data()), 4);
        if (file_.gcount() != 4) {
            break;
        }
        uint32_t valLen = Uint32LE(lenBuf.data());

        std::vector<uint8_t> finalVal;
        if (valLen > 0) {
            if (scratchBuf.size() < valLen) {
                scratchBuf.resize(valLen);
            }
            file_.read(reinterpret_cast<char*>(scratchBuf.data()), valLen);
            if (file_.gcount() != valLen) {
                break;
            }
            finalVal.assign(scratchBuf.begin(), scratchBuf.begin() + valLen);
        }

        std::vector<uint8_t> checkBuf(4 + finalIK.size() + 4 + finalVal.size());
        PutUint32LE(checkBuf.data(), keyLen);
        std::copy(finalIK.begin(), finalIK.end(), checkBuf.data() + 4);
        PutUint32LE(checkBuf.data() + 4 + finalIK.size(), valLen);
        if (!finalVal.empty()) {
            std::copy(finalVal.begin(), finalVal.end(), checkBuf.data() + 4 + finalIK.size() + 4);
        }

        uint32_t computed = crc32_ieee(checkBuf.data(), checkBuf.size());
        if (computed != expectedChecksum) {
            break;
        }

        std::vector<uint8_t> uKey;
        uint64_t seq = 0;
        uint8_t kType = 0;
        if (ParseInternalKey(finalIK, uKey, seq, kType)) {
            if (seq > maxSeqNum) {
                maxSeqNum = seq;
            }
            if (kType == TypeDelete) {
                mt->Delete(uKey, seq);
            } else {
                mt->Put(uKey, finalVal, seq);
            }
        }

        validOffset += 4 + 4 + keyLen + 4 + valLen;
    }

    file_.close();
    std::error_code ec;
    std::filesystem::resize_file(path_, validOffset, ec);
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    file_.seekp(validOffset, std::ios::beg);
    file_.seekg(validOffset, std::ios::beg);

    return maxSeqNum;
}

void WAL::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (file_.is_open()) {
        file_.close();
    }
}

std::string WAL::GetPath() const {
    return path_;
}
