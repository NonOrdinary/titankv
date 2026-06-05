#pragma once

#include "MemTable.hpp"
#include <string>
#include <fstream>
#include <cstring>
#include <mutex>
#include <cstdint>

class WAL {
private:
    std::string filePath;
    std::fstream file;
    std::mutex mu;

    uint32_t computeCRC32(const char* data, size_t length);
    void putUint32LE(char* buf, uint32_t val);
    uint32_t getUint32LE(const char* buf);

public:
    WAL(const std::string& path);
    ~WAL();

    bool WriteRecord(const std::string& internalKey, const std::string& value);
    uint64_t Recover(MemTable* mt);
    void Close();
    std::string GetPath() const;
};