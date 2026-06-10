#include "manifest.hpp"
#include <filesystem>
#include <unordered_map>
#include <sstream>
#include <cstdio>

static std::string EscapeString(const std::string& s) {
    std::string res;
    res.reserve(s.size() + 2);
    for (char c : s) {
        if (c == '\\') res += "\\\\";
        else if (c == '"') res += "\\\"";
        else if (c == '\n') res += "\\n";
        else if (c == '\r') res += "\\r";
        else if (c == '\t') res += "\\t";
        else if (static_cast<unsigned char>(c) < 0x20) {
            char buf[8];
            snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
            res += buf;
        } else {
            res += c;
        }
    }
    return res;
}

static std::string UnescapeString(const std::string& s) {
    std::string res;
    res.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            char next = s[i+1];
            if (next == '\\') { res += '\\'; ++i; }
            else if (next == '"') { res += '"'; ++i; }
            else if (next == 'n') { res += '\n'; ++i; }
            else if (next == 'r') { res += '\r'; ++i; }
            else if (next == 't') { res += '\t'; ++i; }
            else if (next == 'u' && i + 5 < s.size()) {
                std::string hex = s.substr(i + 2, 4);
                try {
                    uint32_t val = std::stoul(hex, nullptr, 16);
                    if (val < 0x80) {
                        res += static_cast<char>(val);
                    } else if (val < 0x800) {
                        res += static_cast<char>(0xc0 | (val >> 6));
                        res += static_cast<char>(0x80 | (val & 0x3f));
                    } else {
                        res += static_cast<char>(0xe0 | (val >> 12));
                        res += static_cast<char>(0x80 | ((val >> 6) & 0x3f));
                        res += static_cast<char>(0x80 | (val & 0x3f));
                    }
                } catch (...) {
                    res += s[i];
                }
                i += 5;
            } else {
                res += s[i];
            }
        } else {
            res += s[i];
        }
    }
    return res;
}

static std::string SerializeRecord(const ManifestRecord& rec) {
    return "{\"action\":\"" + EscapeString(rec.action) + "\","
           "\"path\":\"" + EscapeString(rec.path) + "\","
           "\"min_key\":\"" + EscapeString(rec.min_key) + "\","
           "\"max_key\":\"" + EscapeString(rec.max_key) + "\"}";
}

static bool ParseRecord(const std::string& line, ManifestRecord& rec) {
    auto find_val = [&](const std::string& key) -> std::string {
        size_t pos = line.find("\"" + key + "\"");
        if (pos == std::string::npos) return "";
        pos = line.find(":", pos);
        if (pos == std::string::npos) return "";
        size_t start = line.find("\"", pos);
        if (start == std::string::npos) return "";
        size_t end = start + 1;
        while (end < line.size()) {
            if (line[end] == '"') {
                // Check if escaped
                size_t backslashes = 0;
                size_t p = end - 1;
                while (p >= start && line[p] == '\\') {
                    backslashes++;
                    p--;
                }
                if (backslashes % 2 == 0) {
                    break;
                }
            }
            end++;
        }
        if (end >= line.size()) return "";
        return UnescapeString(line.substr(start + 1, end - start - 1));
    };

    rec.action = find_val("action");
    rec.path = find_val("path");
    rec.min_key = find_val("min_key");
    rec.max_key = find_val("max_key");
    return !rec.action.empty() && !rec.path.empty();
}

Manifest::Manifest(const std::string& path) : path_(path) {}

Manifest::~Manifest() {
    Close();
}

std::unique_ptr<Manifest> Manifest::Open(const std::string& path) {
    auto m = std::unique_ptr<Manifest>(new Manifest(path));
    m->file_.open(path, std::ios::binary | std::ios::app);
    if (!m->file_.is_open()) {
        return nullptr;
    }
    return m;
}

bool Manifest::Append(const ManifestRecord& record) {
    std::lock_guard<std::mutex> lock(mu_);
    std::string line = SerializeRecord(record) + "\n";
    if (!file_.write(line.data(), line.size())) {
        return false;
    }
    file_.flush();
    return true;
}

bool Manifest::Compact(const std::vector<ManifestRecord>& activeRecords) {
    std::lock_guard<std::mutex> lock(mu_);
    std::string tempPath = path_ + ".tmp";
    std::ofstream tempFile(tempPath, std::ios::binary | std::ios::trunc);
    if (!tempFile.is_open()) {
        return false;
    }

    for (const auto& rec : activeRecords) {
        ManifestRecord copyRec = rec;
        copyRec.action = "ADD";
        std::string line = SerializeRecord(copyRec) + "\n";
        if (!tempFile.write(line.data(), line.size())) {
            return false;
        }
    }

    tempFile.flush();
    tempFile.close();

    if (file_.is_open()) {
        file_.close();
    }

    std::error_code ec;
    std::filesystem::rename(tempPath, path_, ec);
    if (ec) {
        return false;
    }

    file_.open(path_, std::ios::binary | std::ios::app);
    return file_.is_open();
}

void Manifest::Close() {
    std::lock_guard<std::mutex> lock(mu_);
    if (file_.is_open()) {
        file_.close();
    }
}

std::vector<ManifestRecord> RecoverManifest(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return {};
    }

    std::vector<ManifestRecord> records;
    std::unordered_map<std::string, size_t> pathToIdx;
    std::vector<bool> isDead;

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        ManifestRecord rec;
        if (!ParseRecord(line, rec)) {
            break;
        }

        if (rec.action == "ADD") {
            records.push_back(rec);
            isDead.push_back(false);
            pathToIdx[rec.path] = records.size() - 1;
        } else if (rec.action == "REMOVE") {
            auto it = pathToIdx.find(rec.path);
            if (it != pathToIdx.end()) {
                isDead[it->second] = true;
            }
        }
    }

    std::vector<ManifestRecord> active;
    for (size_t i = 0; i < records.size(); i++) {
        if (!isDead[i]) {
            active.push_back(records[i]);
        }
    }
    return active;
}
