#pragma once

#include <cstdint>
#include <cstddef>

namespace flexql {

class PrimaryIndex {
public:
    PrimaryIndex();
    ~PrimaryIndex();

    bool contains(const char* key) const;
    void put(const char* key, std::uint64_t row_id);
    bool get(const char* key, std::uint64_t* row_id) const;
    void clear();
    std::size_t size() const;

private:
    struct Node;

    Node* root_;
    std::size_t size_;

    static int key_cmp(const char* a, const char* b);
    static char* key_dup(const char* s);

    static Node* create_node(bool leaf);
    static void free_node_recursive(Node* node);

    bool insert_recursive(Node* node,
                          char* key,
                          std::uint64_t row_id,
                          char** split_key,
                          Node** split_right,
                          bool* replaced);
    bool find_leaf(Node* node, const char* key, std::uint64_t* row_id) const;
};

}  // namespace flexql
