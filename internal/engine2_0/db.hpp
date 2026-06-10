#pragma once

#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include "memtable.hpp"
#include "wal.hpp"
#include "manifest.hpp"
#include "sstable_reader.hpp"
#include "channel.hpp"

struct FlushTask {
    std::shared_ptr<MemTable> mt;
    std::unique_ptr<WAL> wal;
};

class DB {
public:
    static std::unique_ptr<DB> Open(const std::string& dir);
    ~DB();

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    bool Put(const std::string& key, const std::vector<uint8_t>& value);
    bool Delete(const std::string& key);
    bool Get(const std::string& key, std::vector<uint8_t>& val, bool& exists) const;
    bool GetAt(const std::string& key, uint64_t targetSeqNum, std::vector<uint8_t>& val, bool& exists) const;
    
    bool Compact();
    void Close();

    void SetMaxMemTableSize(size_t size);
    uint64_t GetNextSeqNum() const;

private:
    explicit DB(const std::string& dir);

    void triggerFlush(); // Assumes mu_ is locked, unlocks it internally
    void flushWorker();

    std::string dir_;
    std::shared_ptr<MemTable> activeMemTable_;
    std::shared_ptr<MemTable> immutableMemTable_;
    std::unique_ptr<WAL> activeWAL_;
    std::unique_ptr<Manifest> manifest_;
    bool isCompacting_;
    size_t maxMemtableSize_;
    std::atomic<uint64_t> nextSeqNum_;

    std::vector<std::shared_ptr<SSTableReader>> sstables_;
    
    Channel<FlushTask> flushChan_;
    std::thread flushThread_;
    mutable std::shared_mutex mu_;
};
