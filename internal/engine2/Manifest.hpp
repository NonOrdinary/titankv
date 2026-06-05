#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <fstream>

struct ManifestRecord {
    std::string Action;
    std::string Path;
    std::string MinKey;
    std::string MaxKey;
};

class Manifest {
private:
    std::ofstream file;
    std::mutex mu;
    std::string path;

    std::string serialize(const ManifestRecord& record);

public:
    Manifest(const std::string& manifestPath);
    ~Manifest();

    bool Append(const ManifestRecord& record);
    bool Compact(const std::vector<ManifestRecord>& activeRecords);
    void Close();
};

std::vector<ManifestRecord> RecoverManifest(const std::string& path);