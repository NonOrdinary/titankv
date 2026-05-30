#include "SkipList.hpp"
#include <cstdlib>
#include <ctime>


Node::Node(const std::string& k, const std::string& v, int level)
    : key(k), value(v), forward(level + 1, nullptr) {}

SkipList::SkipList() : level(0) {
    std::srand(std::time(nullptr));
    head = new Node("", "", MAX_LEVEL);
}

SkipList::~SkipList() {
    Node* current = head;
    while (current != nullptr) {
        Node* next = current->forward[0];
        delete current;
        current = next;
    }
}

int SkipList::randomLevel() {
    int lvl = 0;
    while ((std::rand() % 2) == 0 && lvl < MAX_LEVEL - 1) {
        lvl++;
    }
    return lvl;
}

std::tuple<std::string, uint8_t, bool> SkipList::Search(const std::string& searchKey) {
    Node* current = head;

    for (int i = level; i >= 0; i--) {
        while (current->forward[i] != nullptr &&
               CompareInternalKeys(current->forward[i]->key, searchKey) < 0) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if (current != nullptr) {
        auto [userKeyNode, seqNumNode, keyType] = ParseInternalKey(current->key);
        auto [userKeySearch, seqNumSearch, searchKeyType] = ParseInternalKey(searchKey);

        if (userKeyNode == userKeySearch) {
            return {current->value, keyType, true};
        }
    }

    return {"", 0, false};
}

void SkipList::Insert(const std::string& internalKey, const std::string& value) {
    std::vector<Node*> update(MAX_LEVEL, nullptr);
    Node* current = head;

    for (int i = level; i >= 0; i--) {
        while (current->forward[i] != nullptr &&
               CompareInternalKeys(current->forward[i]->key, internalKey) < 0) {
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

    if (lvl > level) {
        for (int i = level + 1; i <= lvl; i++) {
            update[i] = head;
        }
        level = lvl;
    }

    Node* newNode = new Node(internalKey, value, lvl);

    for (int i = 0; i <= lvl; i++) {
        newNode->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = newNode;
    }
}

void SkipList::Iterate(std::function<void(const std::string&, const std::string&)> cb) {
    Node* current = head->forward[0];
    while (current != nullptr) {
        cb(current->key, current->value);
        current = current->forward[0];
    }
}