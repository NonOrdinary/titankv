#pragma once
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class MemTable;

class WAL {
public:
  static std::unique_ptr<WAL> Open(const std::string &path);
  ~WAL();

  WAL(const WAL &) = delete;
  WAL &operator=(const WAL &) = delete;

  bool WriteRecord(const std::vector<uint8_t> &internalKey,
                   const std::vector<uint8_t> &value);
  uint64_t Recover(MemTable *mt);
  void Close();
  std::string GetPath() const;

private:
  explicit WAL(const std::string &path);

  std::string path_;
  std::fstream file_;
  std::mutex mu_;
  std::vector<uint8_t> write_buf_;
};
