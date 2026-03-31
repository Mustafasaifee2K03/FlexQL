#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>

namespace flexql {

// Bump-pointer arena allocator. Eliminates malloc/new during the INSERT hot-path.
// NOT thread-safe — each worker thread should use its own instance (thread_local).
struct Arena {
    char* block{nullptr};
    std::size_t capacity{0};
    std::size_t offset{0};

    Arena() = default;

    explicit Arena(std::size_t cap)
        : block(static_cast<char*>(std::malloc(cap))),
          capacity(block ? cap : 0),
          offset(0) {}

    ~Arena() {
        std::free(block);
    }

    // Non-copyable
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    // Movable
    Arena(Arena&& o) noexcept
        : block(o.block), capacity(o.capacity), offset(o.offset) {
        o.block = nullptr;
        o.capacity = 0;
        o.offset = 0;
    }

    Arena& operator=(Arena&& o) noexcept {
        if (this != &o) {
            std::free(block);
            block = o.block;
            capacity = o.capacity;
            offset = o.offset;
            o.block = nullptr;
            o.capacity = 0;
            o.offset = 0;
        }
        return *this;
    }

    void ensure_capacity(std::size_t cap) {
        if (block && capacity >= cap) {
            return;
        }
        std::free(block);
        block = static_cast<char*>(std::malloc(cap));
        capacity = block ? cap : 0;
        offset = 0;
    }

    // Allocate n bytes, 8-byte aligned. Returns nullptr if arena is exhausted.
    void* alloc(std::size_t n) {
        // Align to 8 bytes
        const std::size_t aligned = (n + 7) & ~static_cast<std::size_t>(7);
        if (offset + aligned > capacity) {
            return nullptr;
        }
        void* ptr = block + offset;
        offset += aligned;
        return ptr;
    }

    // Copy a string of length `len` into the arena. Returns arena pointer.
    char* dup(const char* s, std::size_t len) {
        char* dst = static_cast<char*>(alloc(len + 1));
        if (!dst) {
            return nullptr;
        }
        std::memcpy(dst, s, len);
        dst[len] = '\0';
        return dst;
    }

    // O(1) reset — all memory is "freed" instantly.
    void reset() {
        offset = 0;
    }
};

// Lightweight string view for arena-allocated strings.
struct ArenaStringView {
    const char* data{nullptr};
    std::uint32_t length{0};

    bool empty() const { return length == 0; }
};

// Fast INSERT statement parsed with arena-backed storage.
struct FastInsertStmt {
    ArenaStringView table_name;
    ArenaStringView* columns{nullptr};   // arena-allocated array (may be null)
    std::uint32_t column_count{0};
    ArenaStringView* values{nullptr};    // flattened arena array: row_count * value_count
    std::uint32_t value_count{0};
    std::uint32_t row_count{0};
    bool has_expires_at{false};
    std::int64_t expires_at{0};
    bool has_ttl{false};
    std::int64_t ttl_seconds{0};
};

}  // namespace flexql
