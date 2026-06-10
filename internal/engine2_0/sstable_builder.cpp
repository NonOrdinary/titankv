#include "sstable_builder.hpp"
#include "internal_key.hpp"
#include <algorithm>
#include <stdexcept>

static void PutUint32LE(uint8_t* buf, uint32_t val) {
    buf[0] = static_cast<uint8_t>(val);
    buf[1] = static_cast<uint8_t>(val >> 8);
    buf[2] = static_cast<uint8_t>(val >> 16);
    buf[3] = static_cast<uint8_t>(val >> 24);
}

SSTableBuilder::SSTableBuilder(const std::string& path)
    : offset_(0),
      blockBytesWritten_(0),
      bloom_(std::make_unique<BloomFilter>(4096, 3)),
      scratchBuf_(1024) {
    file_.open(path, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        throw std::runtime_error("failed to open sstable file for writing: " + path);
    }
}

SSTableBuilder::~SSTableBuilder() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool SSTableBuilder::Add(const std::vector<uint8_t>& internalKey, const std::vector<uint8_t>& value) {
    std::vector<uint8_t> userKey;
    uint64_t seqNum = 0;
    uint8_t keyType = 0;
    if (!ParseInternalKey(internalKey, userKey, seqNum, keyType)) {
        return false;
    }

    if (minKey_.empty() || userKey < minKey_) {
        minKey_ = userKey;
    }
    if (maxKey_.empty() || userKey > maxKey_) {
        maxKey_ = userKey;
    }

    bloom_->Add(std::string(userKey.begin(), userKey.end()));

    if (blockBytesWritten_ == 0) {
        index_.push_back({userKey, offset_});
    }

    size_t recordSize = 4 + internalKey.size() + 4 + value.size();
    if (scratchBuf_.size() < recordSize) {
        scratchBuf_.resize(recordSize);
    }

    PutUint32LE(scratchBuf_.data(), static_cast<uint32_t>(internalKey.size()));
    size_t offset = 4;

    std::copy(internalKey.begin(), internalKey.end(), scratchBuf_.data() + offset);
    offset += internalKey.size();

    PutUint32LE(scratchBuf_.data() + offset, static_cast<uint32_t>(value.size()));
    offset += 4;

    if (!value.empty()) {
        std::copy(value.begin(), value.end(), scratchBuf_.data() + offset);
    }

    if (!file_.write(reinterpret_cast<const char*>(scratchBuf_.data()), recordSize)) {
        return false;
    }

    offset_ += static_cast<uint32_t>(recordSize);
    blockBytesWritten_ += static_cast<uint32_t>(recordSize);

    if (blockBytesWritten_ >= blockSize) {
        blockBytesWritten_ = 0;
    }

    return true;
}

bool SSTableBuilder::Finish() {
    uint32_t bloomStartOffset = offset_;
    const auto& bloomBytes = bloom_->Bytes();
    if (!file_.write(reinterpret_cast<const char*>(bloomBytes.data()), bloomBytes.size())) {
        return false;
    }
    offset_ += static_cast<uint32_t>(bloomBytes.size());

    uint32_t indexStartOffset = offset_;
    uint8_t countBuf[4];
    PutUint32LE(countBuf, static_cast<uint32_t>(index_.size()));
    if (!file_.write(reinterpret_cast<const char*>(countBuf), 4)) {
        return false;
    }
    offset_ += 4;

    for (const auto& entry : index_) {
        size_t entrySize = 4 + entry.Key.size() + 4;
        if (scratchBuf_.size() < entrySize) {
            scratchBuf_.resize(entrySize);
        }

        PutUint32LE(scratchBuf_.data(), static_cast<uint32_t>(entry.Key.size()));
        std::copy(entry.Key.begin(), entry.Key.end(), scratchBuf_.data() + 4);
        PutUint32LE(scratchBuf_.data() + 4 + entry.Key.size(), entry.Offset);

        if (!file_.write(reinterpret_cast<const char*>(scratchBuf_.data()), entrySize)) {
            return false;
        }
        offset_ += static_cast<uint32_t>(entrySize);
    }

    uint32_t metaStartOffset = offset_;
    size_t metaSize = 4 + minKey_.size() + 4 + maxKey_.size();
    if (scratchBuf_.size() < metaSize) {
        scratchBuf_.resize(metaSize);
    }

    PutUint32LE(scratchBuf_.data(), static_cast<uint32_t>(minKey_.size()));
    size_t metaOffset = 4;
    std::copy(minKey_.begin(), minKey_.end(), scratchBuf_.data() + metaOffset);
    metaOffset += minKey_.size();

    PutUint32LE(scratchBuf_.data() + metaOffset, static_cast<uint32_t>(maxKey_.size()));
    metaOffset += 4;
    std::copy(maxKey_.begin(), maxKey_.end(), scratchBuf_.data() + metaOffset);

    if (!file_.write(reinterpret_cast<const char*>(scratchBuf_.data()), metaSize)) {
        return false;
    }
    offset_ += static_cast<uint32_t>(metaSize);

    uint8_t footerBuf[16];
    PutUint32LE(footerBuf, bloomStartOffset);
    PutUint32LE(footerBuf + 4, indexStartOffset);
    PutUint32LE(footerBuf + 8, metaStartOffset);
    PutUint32LE(footerBuf + 12, 0xABCD1234U);

    if (!file_.write(reinterpret_cast<const char*>(footerBuf), 16)) {
        return false;
    }

    file_.flush();
    file_.close();
    return true;
}
