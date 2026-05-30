#pragma once

#include <string>
#include <vector>
#include <tuple>
#include <cstdint>
#include <functional>

int CompareInternalKeys(const std::string& a, const std::string& b);
std::tuple<std::string, std::string, uint8_t> ParseInternalKey(const std::string& internalKey);

const int MAX_LEVEL = 32;

struct Node {
    std::string key;
    std::string value;
    std::vector<Node*> forward;

    Node(const std::string& k, const std::string& v, int level);
};

class SkipList {
private:
    Node* head;
    int level;

    int randomLevel();

public:
    SkipList();
    ~SkipList();

    std::tuple<std::string, uint8_t, bool> Search(const std::string& searchKey);
    void Insert(const std::string& internalKey, const std::string& value);
    void Iterate(std::function<void(const std::string&, const std::string&)> cb);
};