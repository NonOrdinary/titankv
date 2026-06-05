#pragma once

#include "MemTable.hpp"
#include "InternalKey.hpp"
#include "Manifest.hpp"
#include "SSTable.hpp"
#include <string>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <tuple>
#include <cstdint>
#include <filesystem>

// struct ManifestRecord {
//     std::string Action;
//     std::string Path;
//     std::string MinKey;
//     std::string MaxKey;
// };

// class Manifest {
// public:
//     Manifest(const std::string& path) {}
//     void Append(const ManifestRecord& rec) {}
//     void Close() {}
// };

class WAL {
public:
    uint64_t Recover(MemTable* mt) { return 0; }
    bool WriteRecord(const std::string& key, const std::string& val) { return true; }
    void Close() {}
    std::string GetPath() { return ""; }
};

class SSTableReader {
public:
    std::string MinKey;
    std::string MaxKey;
    std::tuple<std::string, bool, bool, bool> Get(const std::string& key, uint64_t seq) { return {"", false, false, true}; }
    void Close() {}
};

class SSTableBuilder {
public:
    SSTableBuilder(const std::string& path) {}
    void Add(const std::string& key, const std::string& val) {}
    void Finish() {}
};

struct FlushTask {
    MemTable* mt;
    WAL* wal;
};

class DB {
private:
    std::shared_mutex mu;
    std::string dir;
    MemTable* activeMemTable;
    MemTable* immutableMemTable;
    WAL* activeWAL;
    Manifest* manifest;
    bool isCompacting;
    size_t maxMemtableSize;
    std::atomic<uint64_t> nextSeqNum;
    std::vector<SSTableReader*> sstables;

    std::queue<FlushTask*> flushQueue;
    std::mutex queueMu;
    std::condition_variable queueCv;
    bool isClosed;
    std::thread flushThread;

    void triggerFlush();
    void flushWorker();

public:
    DB(const std::string& dbDir);
    ~DB();

    static DB* Open(const std::string& dir);
    bool Put(const std::string& key, const std::string& value);
    bool Delete(const std::string& key);
    std::tuple<std::string, bool, bool> Get(const std::string& key);
    std::tuple<std::string, bool, bool> GetAt(const std::string& key, uint64_t targetSeqNum);
    void Compact();
    void Close();
};