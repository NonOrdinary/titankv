#pragma once

#include <vector>
#include <cstdint>
#include <random>
#include "internal_key.hpp"

struct Node {
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
    std::vector<Node*> forward;

    Node(std::vector<uint8_t> k, std::vector<uint8_t> v, int level)
        : key(std::move(k)), value(std::move(v)), forward(level + 1, nullptr) {}
};

class SkipList {
public:
    static constexpr int maxLevel = 32;

    SkipList() : level_(0) {
        head_ = new Node({}, {}, maxLevel - 1);
    }

    ~SkipList() {
        Node* current = head_;
        while (current != nullptr) {
            Node* next = current->forward[0];
            delete current;
            current = next;
        }
    }

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    bool Search(const std::vector<uint8_t>& searchKey, std::vector<uint8_t>& val, uint8_t& keyType) const {
        Node* current = head_;

        for (int i = level_; i >= 0; i--) {
            while (current->forward[i] != nullptr && CompareInternalKeys(current->forward[i]->key, searchKey) < 0) {
                current = current->forward[i];
            }
        }

        current = current->forward[0];

        if (current != nullptr) {
            std::vector<uint8_t> userKeyNode;
            uint64_t seqNumNode;
            uint8_t keyTypeNode;
            ParseInternalKey(current->key, userKeyNode, seqNumNode, keyTypeNode);

            std::vector<uint8_t> userKeySearch;
            uint64_t seqNumSearch;
            uint8_t keyTypeSearch;
            ParseInternalKey(searchKey, userKeySearch, seqNumSearch, keyTypeSearch);

            if (userKeyNode == userKeySearch) {
                val = current->value;
                keyType = keyTypeNode;
                return true;
            }
        }

        return false;
    }

    void Insert(const std::vector<uint8_t>& internalKey, const std::vector<uint8_t>& value) {
        std::vector<Node*> update(maxLevel, nullptr);
        Node* current = head_;

        for (int i = level_; i >= 0; i--) {
            while (current->forward[i] != nullptr && CompareInternalKeys(current->forward[i]->key, internalKey) < 0) {
                current = current->forward[i];
            }
            update[i] = current;
        }

        current = current->forward[0];

        if (current != nullptr && current->key == internalKey) {
            current->value = value;
            return;
        }

        int lvl = randomLevel();

        if (lvl > level_) {
            for (int i = level_ + 1; i <= lvl; i++) {
                update[i] = head_;
            }
            level_ = lvl;
        }

        Node* newNodePtr = new Node(internalKey, value, lvl);

        for (int i = 0; i <= lvl; i++) {
            newNodePtr->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNodePtr;
        }
    }

    template <typename Callback>
    void Iterate(Callback cb) const {
        Node* current = head_->forward[0];
        while (current != nullptr) {
            cb(current->key, current->value);
            current = current->forward[0];
        }
    }

private:
    int randomLevel() const {
        int level = 0;
        thread_local std::mt19937 gen(std::random_device{}());
        thread_local std::uniform_real_distribution<float> dis(0.0f, 1.0f);
        while (dis(gen) < 0.5f && level < maxLevel - 1) {
            level++;
        }
        return level;
    }

    Node* head_;
    int level_;
};
