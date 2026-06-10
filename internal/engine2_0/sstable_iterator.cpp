#include "sstable_iterator.hpp"
#include <stdexcept>

static uint32_t Uint32LE(const uint8_t* buf) {
    return static_cast<uint32_t>(buf[0]) |
           (static_cast<uint32_t>(buf[1]) << 8) |
           (static_cast<uint32_t>(buf[2]) << 16) |
           (static_cast<uint32_t>(buf[3]) << 24);
}

SSTableIterator::SSTableIterator(const std::string& path, uint32_t endPos)
    : endPos_(endPos), curr_(0) {
    file_.open(path, std::ios::in | std::ios::binary);
}

SSTableIterator::~SSTableIterator() {
    Close();
}

bool SSTableIterator::Next(IteratorKV& kv) {
    if (curr_ >= endPos_) {
        return false;
    }

    uint8_t lenBuf[4];
    file_.read(reinterpret_cast<char*>(lenBuf), 4);
    if (file_.gcount() != 4) {
        return false;
    }
    uint32_t ikLen = Uint32LE(lenBuf);
    curr_ += 4;

    std::vector<uint8_t> finalIK(ikLen);
    file_.read(reinterpret_cast<char*>(finalIK.data()), ikLen);
    if (file_.gcount() != ikLen) {
        return false;
    }
    curr_ += ikLen;

    file_.read(reinterpret_cast<char*>(lenBuf), 4);
    if (file_.gcount() != 4) {
        return false;
    }
    uint32_t valLen = Uint32LE(lenBuf);
    curr_ += 4;

    std::vector<uint8_t> finalVal(valLen);
    if (valLen > 0) {
        file_.read(reinterpret_cast<char*>(finalVal.data()), valLen);
        if (file_.gcount() != valLen) {
            return false;
        }
        curr_ += valLen;
    }

    kv.InternalKey = std::move(finalIK);
    kv.Value = std::move(finalVal);
    return true;
}

void SSTableIterator::Close() {
    if (file_.is_open()) {
        file_.close();
    }
}
