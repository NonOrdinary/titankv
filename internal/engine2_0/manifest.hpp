#pragma once

#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

struct ManifestRecord {
  std::string action;
  std::string path;
  std::string min_key;
  std::string max_key;
};

class Manifest {
public:
  static std::unique_ptr<Manifest> Open(const std::string &path);
  ~Manifest();

  Manifest(const Manifest &) = delete;
  Manifest &operator=(const Manifest &) = delete;

  bool Append(const ManifestRecord &record);
  bool Compact(const std::vector<ManifestRecord> &activeRecords);
  void Close();

private:
  explicit Manifest(const std::string &path);

  std::string path_;
  std::ofstream file_;
  std::mutex mu_;
};

std::vector<ManifestRecord> RecoverManifest(const std::string &path);
