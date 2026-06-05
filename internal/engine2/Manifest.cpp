#include "Manifest.hpp"
#include <filesystem>
#include <unordered_map>
#include <sstream>

Manifest::Manifest(const std::string& manifestPath) : path(manifestPath) {
    file.open(path, std::ios::out | std::ios::app | std::ios::binary);
}

Manifest::~Manifest() {
    Close();
}

std::string Manifest::serialize(const ManifestRecord& record) {
    // Generates raw JSON matching Go's exact serialization format
    return "{\"action\":\"" + record.Action + 
           "\",\"path\":\"" + record.Path + 
           "\",\"min_key\":\"" + record.MinKey + 
           "\",\"max_key\":\"" + record.MaxKey + "\"}";
}

bool Manifest::Append(const ManifestRecord& record) {
    std::lock_guard<std::mutex> lock(mu);
    if (!file.is_open()) return false;

    std::string data = serialize(record) + "\n";
    file.write(data.data(), data.size());
    file.flush(); // Equates to Go's file.Sync() for stream flushing
    
    return file.good();
}

bool Manifest::Compact(const std::vector<ManifestRecord>& activeRecords) {
    std::lock_guard<std::mutex> lock(mu);

    std::string tempPath = path + ".tmp";
    std::ofstream tempFile(tempPath, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!tempFile.is_open()) return false;

    for (auto rec : activeRecords) {
        rec.Action = "ADD"; // Force compaction history to clean state additions
        std::string data = serialize(rec) + "\n";
        tempFile.write(data.data(), data.size());
    }

    tempFile.flush();
    tempFile.close();

    if (file.is_open()) {
        file.close();
    }

    std::error_code ec;
    std::filesystem::rename(tempPath, path, ec);
    if (ec) return false;

    file.open(path, std::ios::out | std::ios::app | std::ios::binary);
    return file.is_open();
}

void Manifest::Close() {
    std::lock_guard<std::mutex> lock(mu);
    if (file.is_open()) {
        file.close();
    }
}

// Robust custom JSON stream parser to avoid external dependencies
static bool DeserializeRecord(const std::string& line, ManifestRecord& rec) {
    auto extractField = [](const std::string& json, const std::string& key) -> std::string {
        std::string pattern = "\"" + key + "\":\"";
        size_t startPos = json.find(pattern);
        if (startPos == std::string::npos) return "";
        startPos += pattern.length();
        
        size_t endPos = json.find("\"", startPos);
        if (endPos == std::string::npos) return "";
        
        return json.substr(startPos, endPos - startPos);
    };

    rec.Action = extractField(line, "action");
    rec.Path = extractField(line, "path");
    rec.MinKey = extractField(line, "min_key");
    rec.MaxKey = extractField(line, "max_key");

    return !rec.Action.empty() && !rec.Path.empty();
}

std::vector<ManifestRecord> RecoverManifest(const std::string& path) {
    if (!std::filesystem::exists(path)) {
        return {};
    }

    std::ifstream f(path, std::ios::in | std::ios::binary);
    if (!f.is_open()) {
        return {};
    }

    std::vector<ManifestRecord> records;
    std::unordered_map<std::string, size_t> pathToIdx;
    std::unordered_map<size_t, bool> isDead;

    std::string line;
    size_t idx = 0;

    while (std::getline(f, line)) {
        if (line.empty()) continue;
        
        ManifestRecord rec;
        if (!DeserializeRecord(line, rec)) {
            break; 
        }

        if (rec.Action == "ADD") {
            records.push_back(rec);
            pathToIdx[rec.Path] = idx;
            idx++;
        } else if (rec.Action == "REMOVE") {
            if (pathToIdx.find(rec.Path) != pathToIdx.end()) {
                size_t targetIdx = pathToIdx[rec.Path];
                isDead[targetIdx] = true;
            }
        }
    }
    f.close();

    std::vector<ManifestRecord> activeTables;
    for (size_t i = 0; i < records.size(); ++i) {
        if (isDead.find(i) == isDead.end()) {
            activeTables.push_back(records[i]);
        }
    }

    return activeTables;
}