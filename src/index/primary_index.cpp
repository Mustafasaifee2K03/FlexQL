#include "index/primary_index.h"

#include <cstdlib>
#include <cstring>

namespace flexql {

namespace {

constexpr int kOrder = 32;
constexpr int kMaxKeys = kOrder - 1;

}  // namespace

struct PrimaryIndex::Node {
    bool leaf;
    int key_count;
    // One extra temporary slot is required during insert-before-split.
    char* keys[kOrder];
    union {
        Node* children[kOrder + 1];
        std::uint64_t values[kOrder];
    } ptr;
    Node* next;
};

PrimaryIndex::PrimaryIndex() : root_(nullptr), size_(0) {}

PrimaryIndex::~PrimaryIndex() {
    clear();
}

int PrimaryIndex::key_cmp(const char* a, const char* b) {
    if (!a && !b) {
        return 0;
    }
    if (!a) {
        return -1;
    }
    if (!b) {
        return 1;
    }
    return std::strcmp(a, b);
}

char* PrimaryIndex::key_dup(const char* s) {
    if (!s) {
        return nullptr;
    }
    const std::size_t n = std::strlen(s);
    auto* out = static_cast<char*>(std::malloc(n + 1));
    if (!out) {
        return nullptr;
    }
    std::memcpy(out, s, n + 1);
    return out;
}

PrimaryIndex::Node* PrimaryIndex::create_node(bool leaf) {
    auto* n = static_cast<Node*>(std::calloc(1, sizeof(Node)));
    if (!n) {
        return nullptr;
    }
    n->leaf = leaf;
    n->key_count = 0;
    n->next = nullptr;
    return n;
}

void PrimaryIndex::free_node_recursive(Node* node) {
    if (!node) {
        return;
    }
    if (!node->leaf) {
        for (int i = 0; i <= node->key_count; ++i) {
            free_node_recursive(node->ptr.children[i]);
        }
    }
    for (int i = 0; i < node->key_count; ++i) {
        std::free(node->keys[i]);
    }
    std::free(node);
}

bool PrimaryIndex::contains(const char* key) const {
    std::uint64_t ignored = 0;
    return get(key, &ignored);
}

bool PrimaryIndex::find_leaf(Node* node, const char* key, std::uint64_t* row_id) const {
    if (!node) {
        return false;
    }
    Node* cur = node;
    while (!cur->leaf) {
        int i = 0;
        while (i < cur->key_count && key_cmp(key, cur->keys[i]) >= 0) {
            ++i;
        }
        cur = cur->ptr.children[i];
    }
    for (int i = 0; i < cur->key_count; ++i) {
        if (key_cmp(key, cur->keys[i]) == 0) {
            if (row_id) {
                *row_id = cur->ptr.values[i];
            }
            return true;
        }
    }
    return false;
}

bool PrimaryIndex::insert_recursive(Node* node,
                                    char* key,
                                    std::uint64_t row_id,
                                    char** split_key,
                                    Node** split_right,
                                    bool* replaced) {
    if (node->leaf) {
        int pos = 0;
        while (pos < node->key_count && key_cmp(node->keys[pos], key) < 0) {
            ++pos;
        }

        if (pos < node->key_count && key_cmp(node->keys[pos], key) == 0) {
            node->ptr.values[pos] = row_id;
            if (replaced) {
                *replaced = true;
            }
            std::free(key);
            return false;
        }

        for (int i = node->key_count; i > pos; --i) {
            node->keys[i] = node->keys[i - 1];
            node->ptr.values[i] = node->ptr.values[i - 1];
        }
        node->keys[pos] = key;
        node->ptr.values[pos] = row_id;
        node->key_count++;

        if (node->key_count <= kMaxKeys) {
            return false;
        }

        const int split = node->key_count / 2;
        Node* right = create_node(true);
        if (!right) {
            return false;
        }

        right->key_count = node->key_count - split;
        for (int i = 0; i < right->key_count; ++i) {
            right->keys[i] = node->keys[split + i];
            right->ptr.values[i] = node->ptr.values[split + i];
        }
        node->key_count = split;

        right->next = node->next;
        node->next = right;

        *split_key = key_dup(right->keys[0]);
        *split_right = right;
        return true;
    }

    int child_idx = 0;
    while (child_idx < node->key_count && key_cmp(key, node->keys[child_idx]) >= 0) {
        ++child_idx;
    }

    char* child_split_key = nullptr;
    Node* child_split_right = nullptr;
    if (!insert_recursive(node->ptr.children[child_idx], key, row_id, &child_split_key, &child_split_right, replaced)) {
        return false;
    }

    for (int i = node->key_count; i > child_idx; --i) {
        node->keys[i] = node->keys[i - 1];
        node->ptr.children[i + 1] = node->ptr.children[i];
    }
    node->keys[child_idx] = child_split_key;
    node->ptr.children[child_idx + 1] = child_split_right;
    node->key_count++;

    if (node->key_count <= kMaxKeys) {
        return false;
    }

    const int mid = node->key_count / 2;
    Node* right = create_node(false);
    if (!right) {
        return false;
    }

    *split_key = key_dup(node->keys[mid]);

    right->key_count = node->key_count - mid - 1;
    for (int i = 0; i < right->key_count; ++i) {
        right->keys[i] = node->keys[mid + 1 + i];
    }
    for (int i = 0; i < right->key_count + 1; ++i) {
        right->ptr.children[i] = node->ptr.children[mid + 1 + i];
    }

    std::free(node->keys[mid]);
    node->key_count = mid;

    *split_right = right;
    return true;
}

void PrimaryIndex::put(const char* key, std::uint64_t row_id) {
    if (!key) {
        return;
    }

    char* owned = key_dup(key);
    if (!owned) {
        return;
    }

    if (!root_) {
        root_ = create_node(true);
        if (!root_) {
            std::free(owned);
            return;
        }
        root_->keys[0] = owned;
        root_->ptr.values[0] = row_id;
        root_->key_count = 1;
        size_ = 1;
        return;
    }

    bool replaced = false;
    char* split_key = nullptr;
    Node* split_right = nullptr;
    const bool split_root = insert_recursive(root_, owned, row_id, &split_key, &split_right, &replaced);
    if (!replaced) {
        size_++;
    }

    if (split_root) {
        Node* new_root = create_node(false);
        if (!new_root) {
            return;
        }
        new_root->keys[0] = split_key;
        new_root->ptr.children[0] = root_;
        new_root->ptr.children[1] = split_right;
        new_root->key_count = 1;
        root_ = new_root;
    }
}

bool PrimaryIndex::get(const char* key, std::uint64_t* row_id) const {
    if (!key || !root_) {
        return false;
    }
    return find_leaf(root_, key, row_id);
}

void PrimaryIndex::clear() {
    free_node_recursive(root_);
    root_ = nullptr;
    size_ = 0;
}

std::size_t PrimaryIndex::size() const {
    return size_;
}

}  // namespace flexql
