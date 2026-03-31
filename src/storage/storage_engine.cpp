#include "storage/storage_engine.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include <string_view>
#include <thread>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils/string_utils.h"
#include "utils/time_utils.h"

namespace fs = std::filesystem;

namespace flexql {

namespace {

constexpr std::uint32_t kColumnMagic = 0x46584C43;  // FXLC
constexpr std::uint32_t kDefaultInitialRows = 4096;
constexpr std::uint32_t kVarcharFixedWidth = 96;
constexpr std::uint32_t kMemTableFlushRows = 1000000;
constexpr std::uint64_t kMemTableDefaultArenaBytes = 512ULL * 1024ULL * 1024ULL;

struct ProfileCounter {
    std::atomic<std::uint64_t> calls{0};
    std::atomic<std::uint64_t> total_ns{0};
};

ProfileCounter g_prof_insert_rows_fast;
ProfileCounter g_prof_insert_rows_fast_lock_wait;
ProfileCounter g_prof_flush_memtable;
ProfileCounter g_prof_flush_row_materialize;
ProfileCounter g_prof_grow_capacity;

inline bool storage_profile_enabled() {
    static const bool enabled = []() {
        const char* env = std::getenv("FLEXQL_PROFILE");
        return env && *env != '\0' && std::strcmp(env, "0") != 0;
    }();
    return enabled;
}

inline void profile_add(ProfileCounter* counter, std::uint64_t ns) {
    if (!counter) {
        return;
    }
    counter->calls.fetch_add(1, std::memory_order_relaxed);
    counter->total_ns.fetch_add(ns, std::memory_order_relaxed);
}

struct ScopedProfile {
    ProfileCounter* counter{nullptr};
    std::chrono::steady_clock::time_point start{};

    explicit ScopedProfile(ProfileCounter* c)
        : counter(storage_profile_enabled() ? c : nullptr),
          start(counter ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{}) {}

    ~ScopedProfile() {
        if (!counter) {
            return;
        }
        const auto end = std::chrono::steady_clock::now();
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        profile_add(counter, static_cast<std::uint64_t>(ns));
    }
};

inline void dump_storage_profile() {
    if (!storage_profile_enabled()) {
        return;
    }

    const auto emit = [](const char* name, const ProfileCounter& c) {
        const std::uint64_t calls = c.calls.load(std::memory_order_relaxed);
        const std::uint64_t ns = c.total_ns.load(std::memory_order_relaxed);
        const double ms = static_cast<double>(ns) / 1'000'000.0;
        std::cerr << "[PROFILE] " << name << " calls=" << calls << " total_ms=" << ms;
        if (calls > 0) {
            std::cerr << " avg_us=" << (static_cast<double>(ns) / 1000.0 / static_cast<double>(calls));
        }
        std::cerr << "\n";
    };

    std::cerr << "[PROFILE] ---- StorageEngine Hot Path ----\n";
    emit("insert_rows_fast", g_prof_insert_rows_fast);
    emit("insert_rows_fast_lock_wait", g_prof_insert_rows_fast_lock_wait);
    emit("flush_memtable_buf_locked", g_prof_flush_memtable);
    emit("flush_memtable_row_materialize", g_prof_flush_row_materialize);
    emit("grow_table_capacity", g_prof_grow_capacity);
}

struct ColumnHeader {
    std::uint32_t magic;
    std::uint32_t type;
    std::uint64_t rows;
    std::uint64_t capacity_rows;
    std::uint32_t width;
    std::uint32_t reserved;
};

inline std::uint32_t width_for_type(DataType t) {
    switch (t) {
        case DataType::INT:
        case DataType::DECIMAL:
        case DataType::DATETIME:
            return 8;
        case DataType::VARCHAR:
            return kVarcharFixedWidth;
    }
    return 8;
}

// ---- Micro-optimized integer/double parsers for the hot path ----
inline std::int64_t fast_parse_int64(const char* s) {
    std::int64_t val = 0;
    bool neg = false;
    if (*s == '-') { neg = true; ++s; }
    else if (*s == '+') { ++s; }
    while (*s >= '0' && *s <= '9') {
        val = val * 10 + (*s - '0');
        ++s;
    }
    return neg ? -val : val;
}

inline bool fast_parse_int64_strict(const char* s, std::int64_t* out) {
    if (!s || !*s) {
        return false;
    }
    const char* p = s;
    bool neg = false;
    if (*p == '-') {
        neg = true;
        ++p;
    } else if (*p == '+') {
        ++p;
    }
    if (*p < '0' || *p > '9') {
        return false;
    }

    std::int64_t val = 0;
    while (*p >= '0' && *p <= '9') {
        val = val * 10 + static_cast<std::int64_t>(*p - '0');
        ++p;
    }
    if (*p != '\0') {
        return false;
    }
    if (out) {
        *out = neg ? -val : val;
    }
    return true;
}

inline double fast_parse_double(const char* s) {
    // Use strtod for doubles — no safe inline shortcut for full precision
    return std::strtod(s, nullptr);
}

inline char* fast_itoa(char* buffer, std::int64_t value) {
    std::uint64_t u = static_cast<std::uint64_t>(value);
    if (value < 0) {
        *buffer++ = '-';
        u = ~u + 1;
    }

    char tmp[20];
    int n = 0;
    do {
        tmp[n++] = static_cast<char>('0' + (u % 10));
        u /= 10;
    } while (u != 0);

    while (n > 0) {
        *buffer++ = tmp[--n];
    }
    return buffer;
}

inline char* fast_u64toa(char* buffer, std::uint64_t value) {
    char tmp[20];
    int n = 0;
    do {
        tmp[n++] = static_cast<char>('0' + (value % 10));
        value /= 10;
    } while (value != 0);

    while (n > 0) {
        *buffer++ = tmp[--n];
    }
    return buffer;
}

inline char* fast_dtoa(char* buffer, double value) {
    if (!std::isfinite(value)) {
        if (std::isnan(value)) {
            std::memcpy(buffer, "nan", 3);
            return buffer + 3;
        }
        if (value < 0) {
            std::memcpy(buffer, "-inf", 4);
            return buffer + 4;
        }
        std::memcpy(buffer, "inf", 3);
        return buffer + 3;
    }

    auto [ptr, ec] = std::to_chars(buffer, buffer + 48, value, std::chars_format::general);
    if (ec == std::errc()) {
        return ptr;
    }

    // Conservative fallback for very rare conversion failures.
    const std::int64_t integral = static_cast<std::int64_t>(value);
    const double frac = std::abs(value - static_cast<double>(integral));
    ptr = fast_itoa(buffer, integral);
    *ptr++ = '.';
    const std::uint64_t frac_scaled = static_cast<std::uint64_t>(frac * 1000000.0 + 0.5);
    char frac_buf[16];
    char* frac_end = fast_u64toa(frac_buf, frac_scaled);
    const std::size_t frac_len = static_cast<std::size_t>(frac_end - frac_buf);
    for (std::size_t i = frac_len; i < 6; ++i) {
        *ptr++ = '0';
    }
    std::memcpy(ptr, frac_buf, frac_len);
    return ptr + frac_len;
}

inline void append_escaped(std::string& out, const char* data, std::size_t len) {
    for (std::size_t i = 0; i < len; ++i) {
        const char c = data[i];
        if (c == '\\') {
            out.push_back('\\');
            out.push_back('\\');
        } else if (c == '\t') {
            out.push_back('\\');
            out.push_back('t');
        } else if (c == '\n') {
            out.push_back('\\');
            out.push_back('n');
        } else {
            out.push_back(c);
        }
    }
}

inline ColumnHeader* header_ptr(void* base) {
    return reinterpret_cast<ColumnHeader*>(base);
}

inline const ColumnHeader* header_ptr(const void* base) {
    return reinterpret_cast<const ColumnHeader*>(base);
}

inline std::uint8_t* data_ptr(void* base) {
    return reinterpret_cast<std::uint8_t*>(base) + sizeof(ColumnHeader);
}

inline const std::uint8_t* data_ptr(const void* base) {
    return reinterpret_cast<const std::uint8_t*>(base) + sizeof(ColumnHeader);
}

std::size_t env_to_bytes(const char* env_name, std::size_t default_bytes, std::size_t unit_multiplier) {
    const char* raw = std::getenv(env_name);
    if (!raw || *raw == '\0') {
        return default_bytes;
    }
    try {
        const auto units = static_cast<std::size_t>(std::stoull(raw));
        if (units == 0) {
            return default_bytes;
        }
        return units * unit_multiplier;
    } catch (...) {
        return default_bytes;
    }
}

std::uint64_t estimate_memtable_row_bytes(const Table* table) {
    if (!table) {
        return 1024;
    }

    std::uint64_t bytes = 0;
    for (const auto& col : table->columns) {
        switch (col.type) {
            case DataType::INT:
            case DataType::DECIMAL:
                bytes += 24;
                break;
            case DataType::DATETIME:
                bytes += 28;
                break;
            case DataType::VARCHAR:
                bytes += 48;
                break;
        }
    }

    // Add per-column length prefix and a small safety margin.
    bytes += static_cast<std::uint64_t>(table->columns.size()) * sizeof(std::uint16_t);
    bytes += 64;
    return bytes;
}

inline bool parse_int_view(const ArenaStringView& v, std::int64_t* out) {
    if (!v.data || v.length == 0) {
        return false;
    }
    const char* p = v.data;
    std::size_t len = v.length;
    bool neg = false;

    if (len > 0 && (*p == '-' || *p == '+')) {
        neg = (*p == '-');
        ++p;
        --len;
    }
    if (len == 0) {
        return false;
    }

    std::int64_t val = 0;
    for (std::size_t i = 0; i < len; ++i) {
        if (p[i] < '0' || p[i] > '9') {
            return false;
        }
        val = val * 10 + static_cast<std::int64_t>(p[i] - '0');
    }

    if (out) {
        *out = neg ? -val : val;
    }
    return true;
}

inline bool parse_int_span(const char* p, std::size_t len, std::int64_t* out) {
    if (!p || len == 0) {
        return false;
    }
    bool neg = false;
    if (*p == '-' || *p == '+') {
        neg = (*p == '-');
        ++p;
        --len;
    }
    if (len == 0) {
        return false;
    }

    std::int64_t val = 0;
    for (std::size_t i = 0; i < len; ++i) {
        const char ch = p[i];
        if (ch < '0' || ch > '9') {
            return false;
        }
        val = val * 10 + static_cast<std::int64_t>(ch - '0');
    }

    if (out) {
        *out = neg ? -val : val;
    }
    return true;
}

inline bool parse_decimal_span(const char* p, std::size_t len, double* out) {
    if (!p || len == 0 || !out) {
        return false;
    }

    // Fast path: integer-like token (most benchmark values are integers like "1").
    std::int64_t as_i64 = 0;
    if (parse_int_span(p, len, &as_i64)) {
        *out = static_cast<double>(as_i64);
        return true;
    }

    // General fallback: parse with strtod on a temporary NUL-terminated buffer.
    char tmp[128];
    if (len >= sizeof(tmp)) {
        std::string slow(p, len);
        char* end = nullptr;
        *out = std::strtod(slow.c_str(), &end);
        return end && *end == '\0';
    }
    std::memcpy(tmp, p, len);
    tmp[len] = '\0';
    char* end = nullptr;
    *out = std::strtod(tmp, &end);
    return end && *end == '\0';
}

inline bool parse_datetime_span(const char* p, std::size_t len, std::int64_t* out) {
    if (!out) {
        return false;
    }
    if (parse_int_span(p, len, out)) {
        return true;
    }

    std::string tmp(p, len);
    return utils::parse_datetime_to_epoch(tmp, out);
}

// Kept for legacy — delegates to StorageEngine::decode_buf_row

}  // namespace

StorageEngine::StorageEngine(std::string data_dir)
    : page_cache_limit_bytes_(env_to_bytes("FLEXQL_PAGE_CACHE_MB", 64U * 1024U * 1024U, 1024U * 1024U)),
      write_buffer_limit_bytes_(env_to_bytes("FLEXQL_INSERT_BUFFER_KB", 512U * 1024U, 1024U)),
      page_row_count_(env_to_bytes("FLEXQL_PAGE_ROWS", 128U, 1U)),
      data_dir_(std::move(data_dir)) {
    start_flush_thread();
}

StorageEngine::~StorageEngine() {
    stop_flush_thread();
    (void)flush_all_tables(nullptr);
    dump_storage_profile();

    std::unique_lock lock(tables_mutex_);
    for (auto& it : tables_) {
        auto& table = it.second;
        if (!table) {
            continue;
        }

        if (table->columns_mmap) {
            for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
                auto* col = &table->columns_mmap[i];
                if (col->base && col->mapped_bytes) {
                    ::msync(col->base, col->mapped_bytes, MS_SYNC);
                    ::munmap(col->base, col->mapped_bytes);
                    col->base = nullptr;
                }
                if (col->fd >= 0) {
                    ::close(col->fd);
                    col->fd = -1;
                }
            }
            std::free(table->columns_mmap);
            table->columns_mmap = nullptr;
            table->columns_mmap_count = 0;
        }

        if (table->expires_mmap.base && table->expires_mmap.mapped_bytes) {
            ::msync(table->expires_mmap.base, table->expires_mmap.mapped_bytes, MS_SYNC);
            ::munmap(table->expires_mmap.base, table->expires_mmap.mapped_bytes);
            table->expires_mmap.base = nullptr;
        }
        if (table->expires_mmap.fd >= 0) {
            ::close(table->expires_mmap.fd);
            table->expires_mmap.fd = -1;
        }

        for (int bi = 0; bi < 2; ++bi) {
            auto& buf = table->buffers[bi];
            std::free(buf.data);
            std::free(buf.row_offsets);
            std::free(buf.row_lengths);
            std::free(buf.row_ids);
            std::free(buf.pending_pks);
            std::free(buf.expires_at);
            buf.data = nullptr;
            buf.row_offsets = nullptr;
            buf.row_lengths = nullptr;
            buf.row_ids = nullptr;
            buf.pending_pks = nullptr;
            buf.expires_at = nullptr;
            buf.data_capacity_bytes = 0;
            buf.data_used_bytes.store(0, std::memory_order_release);
            buf.capacity_rows = 0;
            buf.row_count.store(0, std::memory_order_release);
            buf.initialized = false;
        }

        ::pthread_rwlock_destroy(&table->rwlock);
    }
    tables_.clear();
}

// ============================================================================
// decode_buf_row: decode a row from any RawMemTableBuf
// ============================================================================
bool StorageEngine::decode_buf_row(const Table* table, const RawMemTableBuf* buf,
                                   std::uint32_t pending_idx, StoredRow* out, std::string* err) {
    const std::uint32_t pending_count = buf ? buf->row_count.load(std::memory_order_acquire) : 0;
    if (!table || !buf || !out || !buf->initialized || pending_idx >= pending_count ||
        pending_idx >= buf->capacity_rows) {
        if (err) *err = "invalid memtable row decode";
        return false;
    }
    const std::uint64_t start = buf->row_offsets[pending_idx];
    const std::uint64_t end = start + static_cast<std::uint64_t>(buf->row_lengths[pending_idx]);
    const std::uint64_t used_bytes = buf->data_used_bytes.load(std::memory_order_acquire);
    if (end < start || end > used_bytes) {
        if (err) *err = "corrupt memtable row offsets";
        return false;
    }
    const char* p = buf->data + start;
    const char* row_end = buf->data + end;
    if (out->values.size() != table->columns.size()) {
        out->values.resize(table->columns.size());
    }
    for (std::size_t i = 0; i < table->columns.size(); ++i) {
        if (p + static_cast<std::ptrdiff_t>(sizeof(std::uint16_t)) > row_end) {
            if (err) *err = "corrupt memtable row payload";
            return false;
        }
        std::uint16_t n = 0;
        std::memcpy(&n, p, sizeof(n));
        p += sizeof(n);
        if (p + n > row_end) {
            if (err) *err = "corrupt memtable row length";
            return false;
        }
        out->values[i].assign(p, p + n);
        p += n;
    }
    out->expires_at = buf->expires_at[pending_idx];
    out->row_id = buf->row_ids[pending_idx];
    return true;
}

// ============================================================================
// Background flush thread
// ============================================================================
void StorageEngine::start_flush_thread() {
    // Detached flush workers are launched on demand.
}

void StorageEngine::stop_flush_thread() {
    // Detached flush workers are synchronized per-table during flush_all_tables.
}

void StorageEngine::trigger_async_flush(Table* table, int buf_idx) {
    std::thread([this, table, buf_idx] {
        ScopedProfile prof_total(&g_prof_flush_memtable);
        if (!table || buf_idx < 0 || buf_idx >= 2) {
            return;
        }

        // STEP 1: Grow capacity (Requires Write Lock)
        ::pthread_rwlock_wrlock(&table->rwlock);
        RawMemTableBuf* buf = &table->buffers[buf_idx];
        const std::uint32_t pending_count =
            std::min<std::uint32_t>(buf->row_count.load(std::memory_order_acquire), buf->capacity_rows);
        if (pending_count == 0) {
            table->flush_pending.store(false, std::memory_order_release);
            ::pthread_rwlock_unlock(&table->rwlock);
            return;
        }

        std::uint64_t max_row_id = 0;
        const std::uint64_t flush_rows = static_cast<std::uint64_t>(pending_count);
        for (std::uint64_t pending = 0; pending < flush_rows; ++pending) {
            max_row_id = std::max(max_row_id, buf->row_ids[pending]);
        }

        std::string err;
        if (!grow_table_capacity(table, max_row_id + 1, &err)) {
            table->flush_pending.store(false, std::memory_order_release);
            ::pthread_rwlock_unlock(&table->rwlock);
            return;
        }
        ::pthread_rwlock_unlock(&table->rwlock); // <-- DROP THE LOCK!

        // STEP 2: Heavy Disk I/O & Parsing (NO LOCK)
        // Main thread can continue inserting into Buffer B at 0ms latency!
        ScopedProfile prof_rows(&g_prof_flush_row_materialize);
        for (std::uint64_t pending = 0; pending < flush_rows; ++pending) {
            const std::uint64_t row_id = buf->row_ids[pending];
            const char* read_ptr = buf->data + buf->row_offsets[pending];
            
            for (std::size_t col = 0; col < table->columns_mmap_count; ++col) {
                std::uint16_t n = 0;
                std::memcpy(&n, read_ptr, sizeof(n));
                read_ptr += sizeof(n);

                Table::ColumnFile* cfile = &table->columns_mmap[col];
                std::uint8_t* cell = reinterpret_cast<std::uint8_t*>(cfile->base) + 
                                     sizeof(ColumnHeader) + 
                                     row_id * static_cast<std::uint64_t>(cfile->width);

                if (cfile->type == DataType::INT) {
                    std::int64_t v = 0;
                    if (!parse_int_span(read_ptr, n, &v)) {
                        table->flush_pending.store(false, std::memory_order_release);
                        return;
                    }
                    std::memcpy(cell, &v, sizeof(v));
                } else if (cfile->type == DataType::DECIMAL) {
                    double v = 0;
                    if (!parse_decimal_span(read_ptr, n, &v)) {
                        table->flush_pending.store(false, std::memory_order_release);
                        return;
                    }
                    std::memcpy(cell, &v, sizeof(v));
                } else if (cfile->type == DataType::DATETIME) {
                    std::int64_t ts = 0;
                    if (!parse_datetime_span(read_ptr, n, &ts)) {
                        table->flush_pending.store(false, std::memory_order_release);
                        return;
                    }
                    std::memcpy(cell, &ts, sizeof(ts));
                } else if (cfile->type == DataType::VARCHAR) {
                    const std::size_t max_payload = cfile->width > 2 ? static_cast<std::size_t>(cfile->width - 2) : 0;
                    const std::uint16_t vn = static_cast<std::uint16_t>(std::min<std::size_t>(n, max_payload));
                    std::memcpy(cell, &vn, sizeof(vn));
                    if (vn > 0) {
                        std::memcpy(cell + 2, read_ptr, vn);
                    }
                }
                read_ptr += n;
            }

            // Write expiration timestamp
            std::uint8_t* exp_cell = reinterpret_cast<std::uint8_t*>(table->expires_mmap.base) + 
                                     sizeof(ColumnHeader) + 
                                     row_id * static_cast<std::uint64_t>(table->expires_mmap.width);
            std::int64_t exp_val = buf->expires_at[pending];
            std::memcpy(exp_cell, &exp_val, sizeof(exp_val));
        }

        // STEP 3: Finalize (Requires Write Lock to update global row counts)
        ::pthread_rwlock_wrlock(&table->rwlock);
        table->row_count = std::max(table->row_count, max_row_id + 1);
        for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
            header_ptr(table->columns_mmap[i].base)->rows = table->row_count;
        }
        header_ptr(table->expires_mmap.base)->rows = table->row_count;

        buf->row_count.store(0, std::memory_order_release);
        buf->data_used_bytes.store(0, std::memory_order_release);
        table->flush_pending.store(false, std::memory_order_release);
        ::pthread_rwlock_unlock(&table->rwlock);

    }).detach();
}

void StorageEngine::wait_for_pending_flush(Table* table) {
    while (table && table->flush_pending.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
}

CompactRow StorageEngine::build_compact_row(const std::vector<std::string>& values,
                                            const std::vector<ColumnDef>& columns,
                                            std::int64_t expires_at) {
    CompactRow row;
    row.expires_at = expires_at;
    row.values.reserve(columns.size());

    for (std::size_t i = 0; i < values.size() && i < columns.size(); ++i) {
        CompactValue cv{};
        switch (columns[i].type) {
            case DataType::INT:
                cv.int_val = std::strtoll(values[i].c_str(), nullptr, 10);
                break;
            case DataType::DECIMAL:
                cv.decimal_val = std::strtod(values[i].c_str(), nullptr);
                break;
            case DataType::DATETIME: {
                std::int64_t ts = 0;
                if (!utils::parse_datetime_to_epoch(values[i], &ts)) {
                    ts = std::strtoll(values[i].c_str(), nullptr, 10);
                }
                cv.datetime_val = ts;
                break;
            }
            case DataType::VARCHAR:
                cv.varchar.offset = static_cast<std::uint32_t>(row.varchar_data.size());
                cv.varchar.length = static_cast<std::uint32_t>(values[i].size());
                row.varchar_data.append(values[i]);
                break;
        }
        row.values.push_back(cv);
    }

    return row;
}

bool StorageEngine::initialize(std::string* err) {
    try {
        fs::create_directories(fs::path(data_dir_) / "tables");
        fs::create_directories(fs::path(data_dir_) / "indexes");
        fs::create_directories(fs::path(data_dir_) / "wal");

        for (const auto& entry : fs::directory_iterator(fs::path(data_dir_) / "tables")) {
            if (!entry.is_regular_file()) {
                continue;
            }
            const auto p = entry.path();
            if (p.extension() == ".schema") {
                if (!load_table_from_schema(p.string(), err)) {
                    return false;
                }
            }
        }
    } catch (const std::exception& ex) {
        if (err) {
            *err = ex.what();
        }
        return false;
    }
    return true;
}

bool StorageEngine::init_memtable(Table* table, std::string* err) {
    if (!table) {
        if (err) {
            *err = "null table for memtable init";
        }
        return false;
    }
    if (table->buffers[0].initialized) {
        return true;
    }

    std::uint32_t capacity_rows = kMemTableFlushRows;
    if (const char* env_rows = std::getenv("FLEXQL_MEMTABLE_ROWS"); env_rows && *env_rows != '\0') {
        try {
            const auto parsed = static_cast<std::uint32_t>(std::stoul(env_rows));
            if (parsed > 0) {
                capacity_rows = parsed;
            }
        } catch (...) {
        }
    }

    std::uint64_t arena_bytes = kMemTableDefaultArenaBytes;
    if (const char* env_bytes = std::getenv("FLEXQL_MEMTABLE_BYTES"); env_bytes && *env_bytes != '\0') {
        try {
            const auto parsed = static_cast<std::uint64_t>(std::stoull(env_bytes));
            if (parsed > 0) {
                arena_bytes = parsed;
            }
        } catch (...) {
        }
    }

    if (capacity_rows == 0) {
        capacity_rows = 1;
    }

    // Allocate BOTH ping-pong buffers
    for (int bi = 0; bi < 2; ++bi) {
        auto& buf = table->buffers[bi];
        buf.row_offsets = static_cast<std::uint64_t*>(
            std::calloc(capacity_rows, sizeof(std::uint64_t)));
        buf.row_lengths = static_cast<std::uint32_t*>(
            std::calloc(capacity_rows, sizeof(std::uint32_t)));
        buf.row_ids = static_cast<std::uint64_t*>(
            std::calloc(capacity_rows, sizeof(std::uint64_t)));
        buf.pending_pks = static_cast<std::uint64_t*>(
            std::calloc(capacity_rows, sizeof(std::uint64_t)));
        buf.expires_at = static_cast<std::int64_t*>(std::calloc(capacity_rows, sizeof(std::int64_t)));
        buf.data = static_cast<char*>(std::malloc(static_cast<std::size_t>(arena_bytes)));
        if (!buf.row_offsets || !buf.row_lengths || !buf.row_ids || !buf.pending_pks || !buf.expires_at || !buf.data) {
            if (err) {
                *err = "failed to allocate memtable metadata";
            }
            return false;
        }
        buf.data_capacity_bytes = arena_bytes;
        buf.data_used_bytes.store(0, std::memory_order_release);
        buf.capacity_rows = capacity_rows;
        buf.row_count.store(0, std::memory_order_release);
        buf.initialized = true;
    }
    table->active_buf.store(0, std::memory_order_release);
    return true;
}

bool StorageEngine::append_memtable_row_locked(Table* table,
                                               RawMemTableBuf* buf,
                                               const std::vector<std::string>& values,
                                               std::int64_t expires_at,
                                               std::uint64_t* predicted_row_id,
                                               std::string* err) {
    if (!table || !buf || !buf->initialized) {
        if (err) *err = "memtable is not initialized";
        return false;
    }
    const std::uint32_t slot = buf->row_count.load(std::memory_order_acquire);
    if (slot >= buf->capacity_rows) {
        if (err) *err = "memtable is full";
        return false;
    }
    if (values.size() != table->columns.size()) {
        if (err) *err = "column count mismatch";
        return false;
    }

    const std::uint64_t row_id = table->next_row_id.fetch_add(1, std::memory_order_acq_rel);
    std::uint64_t needed_bytes = 0;
    for (const auto& v : values) {
        const std::size_t n = std::min<std::size_t>(v.size(), 65535);
        needed_bytes += sizeof(std::uint16_t) + static_cast<std::uint64_t>(n);
    }

    const std::uint64_t used_bytes = buf->data_used_bytes.load(std::memory_order_acquire);
    if (used_bytes + needed_bytes > buf->data_capacity_bytes) {
        std::uint64_t next_capacity = buf->data_capacity_bytes;
        while (next_capacity < used_bytes + needed_bytes) {
            next_capacity *= 2;
        }
        char* next_data = static_cast<char*>(
            std::realloc(buf->data, static_cast<std::size_t>(next_capacity)));
        if (!next_data) {
            if (err) *err = "failed to grow memtable arena";
            return false;
        }
        buf->data = next_data;
        buf->data_capacity_bytes = next_capacity;
    }

    char* write_ptr = buf->data + used_bytes;
    for (const auto& v : values) {
        const std::uint16_t n = static_cast<std::uint16_t>(std::min<std::size_t>(v.size(), 65535));
        std::memcpy(write_ptr, &n, sizeof(n));
        write_ptr += sizeof(n);
        if (n > 0) {
            std::memcpy(write_ptr, v.data(), n);
            write_ptr += n;
        }
    }

    const std::uint64_t row_end = used_bytes + needed_bytes;
    buf->data_used_bytes.store(row_end, std::memory_order_release);
    buf->row_offsets[slot] = used_bytes;
    buf->row_lengths[slot] = static_cast<std::uint32_t>(needed_bytes);
    buf->row_ids[slot] = row_id;
    buf->expires_at[slot] = expires_at;
    buf->row_count.store(slot + 1, std::memory_order_release);

    if (predicted_row_id) {
        *predicted_row_id = row_id;
    }
    return true;
}

bool StorageEngine::flush_memtable_buf_locked(Table* table, RawMemTableBuf* buf, std::string* err) {
    ScopedProfile prof_total(&g_prof_flush_memtable);
    if (!table || !buf || !buf->initialized) {
        return true;
    }

    const std::uint64_t flush_rows = static_cast<std::uint64_t>(
        std::min<std::uint32_t>(buf->row_count.load(std::memory_order_acquire), buf->capacity_rows));
    if (flush_rows == 0) {
        return true;
    }
    std::uint64_t max_row_id = 0;
    for (std::uint64_t pending = 0; pending < flush_rows; ++pending) {
        max_row_id = std::max(max_row_id, buf->row_ids[pending]);
    }
    if (!grow_table_capacity(table, max_row_id + 1, err)) {
        return false;
    }

    ScopedProfile prof_rows(&g_prof_flush_row_materialize);
    for (std::uint64_t pending = 0; pending < flush_rows; ++pending) {
        const std::uint64_t row_id = buf->row_ids[pending];
        const std::uint64_t row_start = buf->row_offsets[pending];
        const std::uint64_t row_end = row_start + static_cast<std::uint64_t>(buf->row_lengths[pending]);
        const char* read_ptr = buf->data + row_start;
        const char* row_limit = buf->data + row_end;

        for (std::size_t col = 0; col < table->columns_mmap_count; ++col) {
            if (read_ptr + static_cast<std::ptrdiff_t>(sizeof(std::uint16_t)) > row_limit) {
                if (err) *err = "corrupt memtable row payload";
                return false;
            }
            std::uint16_t n = 0;
            std::memcpy(&n, read_ptr, sizeof(n));
            read_ptr += sizeof(n);
            if (read_ptr + n > row_limit) {
                if (err) *err = "corrupt memtable row length";
                return false;
            }

            Table::ColumnFile* cfile = &table->columns_mmap[col];
            std::uint8_t* cell = data_ptr(cfile->base) + row_id * static_cast<std::uint64_t>(cfile->width);
            switch (cfile->type) {
                case DataType::INT: {
                    std::int64_t v = 0;
                    if (!parse_int_span(read_ptr, n, &v)) {
                        if (err) {
                            *err = "invalid INT value while flushing memtable";
                        }
                        return false;
                    }
                    std::memcpy(cell, &v, sizeof(v));
                    break;
                }
                case DataType::DECIMAL: {
                    double v = 0;
                    if (!parse_decimal_span(read_ptr, n, &v)) {
                        if (err) {
                            *err = "invalid DECIMAL value while flushing memtable";
                        }
                        return false;
                    }
                    std::memcpy(cell, &v, sizeof(v));
                    break;
                }
                case DataType::DATETIME: {
                    std::int64_t ts = 0;
                    if (!parse_datetime_span(read_ptr, n, &ts)) {
                        if (err) {
                            *err = "invalid DATETIME value while flushing memtable";
                        }
                        return false;
                    }
                    std::memcpy(cell, &ts, sizeof(ts));
                    break;
                }
                case DataType::VARCHAR: {
                    const std::size_t max_payload = cfile->width > 2 ? static_cast<std::size_t>(cfile->width - 2) : 0;
                    const std::uint16_t out_len = static_cast<std::uint16_t>(std::min<std::size_t>(n, max_payload));
                    std::memcpy(cell, &out_len, sizeof(out_len));
                    if (out_len > 0) {
                        std::memcpy(cell + 2, read_ptr, out_len);
                    }
                    break;
                }
            }
            read_ptr += n;
        }

        std::uint8_t* exp_cell = data_ptr(table->expires_mmap.base) +
                                 row_id * static_cast<std::uint64_t>(table->expires_mmap.width);
        std::memcpy(exp_cell, &buf->expires_at[pending], sizeof(std::int64_t));
    }

    table->row_count = std::max(table->row_count, max_row_id + 1);
    for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
        header_ptr(table->columns_mmap[i].base)->rows = table->row_count;
    }
    header_ptr(table->expires_mmap.base)->rows = table->row_count;

    buf->row_count.store(0, std::memory_order_release);
    buf->data_used_bytes.store(0, std::memory_order_release);
    return true;
}

bool StorageEngine::flush_all_tables(std::string* err) {
    std::shared_lock lock(tables_mutex_);
    for (auto& it : tables_) {
        auto& table = it.second;
        if (!table) {
            continue;
        }
        // Wait for any pending async flush first
        wait_for_pending_flush(table.get());
        ::pthread_rwlock_wrlock(&table->rwlock);
        // Flush both buffers synchronously
        for (int bi = 0; bi < 2; ++bi) {
            const bool ok = flush_memtable_buf_locked(table.get(), &table->buffers[bi], err);
            if (!ok) {
                ::pthread_rwlock_unlock(&table->rwlock);
                return false;
            }
        }
        ::pthread_rwlock_unlock(&table->rwlock);
    }
    return true;
}

bool StorageEngine::map_column_file(Table::ColumnFile* col,
                                    const std::string& path,
                                    DataType type,
                                    std::uint32_t width,
                                    std::uint64_t min_capacity_rows,
                                    std::string* err) {
    if (!col) {
        if (err) {
            *err = "null column mapping";
        }
        return false;
    }

    const bool file_exists = fs::exists(path);
    col->fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (col->fd < 0) {
        if (err) {
            *err = "open failed for " + path + ": " + std::strerror(errno);
        }
        return false;
    }

    std::uint64_t capacity = min_capacity_rows == 0 ? kDefaultInitialRows : min_capacity_rows;
    std::uint64_t existing_size = 0;
    if (file_exists) {
        struct stat st {};
        if (::fstat(col->fd, &st) == 0) {
            existing_size = static_cast<std::uint64_t>(st.st_size);
        }
    }

    if (existing_size >= sizeof(ColumnHeader)) {
        std::uint8_t hdr_buf[sizeof(ColumnHeader)] = {0};
        if (::pread(col->fd, hdr_buf, sizeof(ColumnHeader), 0) == static_cast<ssize_t>(sizeof(ColumnHeader))) {
            const auto* hdr = reinterpret_cast<const ColumnHeader*>(hdr_buf);
            if (hdr->magic == kColumnMagic && hdr->capacity_rows > 0 && hdr->width > 0) {
                capacity = hdr->capacity_rows;
                width = hdr->width;
                type = static_cast<DataType>(hdr->type);
            }
        }
    }

    const std::uint64_t bytes = sizeof(ColumnHeader) + capacity * static_cast<std::uint64_t>(width);
    if (::ftruncate(col->fd, static_cast<off_t>(bytes)) != 0) {
        if (err) {
            *err = "ftruncate failed for " + path + ": " + std::strerror(errno);
        }
        ::close(col->fd);
        col->fd = -1;
        return false;
    }

    void* base = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, col->fd, 0);
    if (base == MAP_FAILED) {
        if (err) {
            *err = "mmap failed for " + path + ": " + std::strerror(errno);
        }
        ::close(col->fd);
        col->fd = -1;
        return false;
    }

    col->base = base;
    col->mapped_bytes = bytes;
    col->capacity_rows = capacity;
    col->width = width;
    col->type = type;
    col->path = path;

    auto* hdr = header_ptr(col->base);
    if (hdr->magic != kColumnMagic) {
        hdr->magic = kColumnMagic;
        hdr->type = static_cast<std::uint32_t>(type);
        hdr->rows = 0;
        hdr->capacity_rows = capacity;
        hdr->width = width;
        hdr->reserved = 0;
        ::msync(col->base, sizeof(ColumnHeader), MS_SYNC);
    }

    return true;
}

bool StorageEngine::grow_table_capacity(Table* table, std::uint64_t min_rows, std::string* err) {
    ScopedProfile prof_total(&g_prof_grow_capacity);
    if (!table) {
        if (err) {
            *err = "null table";
        }
        return false;
    }

    std::uint64_t current = table->expires_mmap.capacity_rows;
    if (current >= min_rows) {
        return true;
    }

    std::uint64_t next = current == 0 ? kDefaultInitialRows : current;
    while (next < min_rows) {
        next *= 2;
    }

    auto remap_one = [&](Table::ColumnFile* col) -> bool {
        if (!col || col->fd < 0) {
            if (err) {
                *err = "invalid column while remapping";
            }
            return false;
        }

        if (col->base && col->mapped_bytes) {
            ::msync(col->base, col->mapped_bytes, MS_SYNC);
            ::munmap(col->base, col->mapped_bytes);
        }

        const std::uint64_t bytes = sizeof(ColumnHeader) + next * static_cast<std::uint64_t>(col->width);
        if (::ftruncate(col->fd, static_cast<off_t>(bytes)) != 0) {
            if (err) {
                *err = "ftruncate failed while growing " + col->path;
            }
            return false;
        }

        void* base = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, col->fd, 0);
        if (base == MAP_FAILED) {
            if (err) {
                *err = "mmap failed while growing " + col->path;
            }
            return false;
        }

        col->base = base;
        col->mapped_bytes = bytes;
        col->capacity_rows = next;

        auto* hdr = header_ptr(col->base);
        hdr->magic = kColumnMagic;
        hdr->type = static_cast<std::uint32_t>(col->type);
        hdr->rows = table->row_count;
        hdr->capacity_rows = next;
        hdr->width = col->width;
        hdr->reserved = 0;
        return true;
    };

    for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
        if (!remap_one(&table->columns_mmap[i])) {
            return false;
        }
    }
    return remap_one(&table->expires_mmap);
}

bool StorageEngine::write_value(Table::ColumnFile* col,
                                std::uint64_t row_id,
                                const std::string& value,
                                std::string* err) {
    if (!col || !col->base || row_id >= col->capacity_rows) {
        if (err) {
            *err = "write_value out of range";
        }
        return false;
    }

    std::uint8_t* cell = data_ptr(col->base) + row_id * static_cast<std::uint64_t>(col->width);
    std::memset(cell, 0, col->width);

    switch (col->type) {
        case DataType::INT: {
            const std::int64_t v = std::strtoll(value.c_str(), nullptr, 10);
            std::memcpy(cell, &v, sizeof(v));
            break;
        }
        case DataType::DECIMAL: {
            const double v = std::strtod(value.c_str(), nullptr);
            std::memcpy(cell, &v, sizeof(v));
            break;
        }
        case DataType::DATETIME: {
            std::int64_t ts = 0;
            if (!utils::parse_datetime_to_epoch(value, &ts)) {
                ts = std::strtoll(value.c_str(), nullptr, 10);
            }
            std::memcpy(cell, &ts, sizeof(ts));
            break;
        }
        case DataType::VARCHAR: {
            std::uint16_t n = static_cast<std::uint16_t>(value.size());
            const std::size_t max_payload = col->width > 2 ? col->width - 2 : 0;
            if (n > max_payload) {
                n = static_cast<std::uint16_t>(max_payload);
            }
            std::memcpy(cell, &n, sizeof(n));
            std::memcpy(cell + 2, value.data(), n);
            break;
        }
    }

    return true;
}

bool StorageEngine::read_value(const Table::ColumnFile* col, std::uint64_t row_id, std::string* out) {
    if (!out) {
        return false;
    }
    out->clear();

    if (!col || !col->base || row_id >= col->capacity_rows) {
        return false;
    }

    const std::uint8_t* cell = data_ptr(col->base) + row_id * static_cast<std::uint64_t>(col->width);
    switch (col->type) {
        case DataType::INT: {
            std::int64_t v = 0;
            std::memcpy(&v, cell, sizeof(v));
            char tmp[32];
            char* end = fast_itoa(tmp, v);
            out->assign(tmp, static_cast<std::size_t>(end - tmp));
            return true;
        }
        case DataType::DECIMAL: {
            double v = 0;
            std::memcpy(&v, cell, sizeof(v));
            char tmp[64];
            char* end = fast_dtoa(tmp, v);
            out->assign(tmp, static_cast<std::size_t>(end - tmp));
            return true;
        }
        case DataType::DATETIME: {
            std::int64_t ts = 0;
            std::memcpy(&ts, cell, sizeof(ts));
            char tmp[32];
            char* end = fast_itoa(tmp, ts);
            out->assign(tmp, static_cast<std::size_t>(end - tmp));
            return true;
        }
        case DataType::VARCHAR: {
            std::uint16_t n = 0;
            std::memcpy(&n, cell, sizeof(n));
            const std::size_t max_payload = col->width > 2 ? col->width - 2 : 0;
            if (n > max_payload) {
                n = static_cast<std::uint16_t>(max_payload);
            }
            out->assign(reinterpret_cast<const char*>(cell + 2), n);
            return true;
        }
    }
    return false;
}

bool StorageEngine::write_expires(Table* table, std::uint64_t row_id, std::int64_t expires_at, std::string* err) {
    if (!table || !table->expires_mmap.base || row_id >= table->expires_mmap.capacity_rows) {
        if (err) {
            *err = "write_expires out of range";
        }
        return false;
    }
    std::uint8_t* cell = data_ptr(table->expires_mmap.base) +
                         row_id * static_cast<std::uint64_t>(table->expires_mmap.width);
    std::memcpy(cell, &expires_at, sizeof(expires_at));
    return true;
}

std::int64_t StorageEngine::read_expires(const Table* table, std::uint64_t row_id) {
    if (!table || !table->expires_mmap.base || row_id >= table->expires_mmap.capacity_rows) {
        return 0;
    }
    std::int64_t ts = 0;
    const std::uint8_t* cell = data_ptr(table->expires_mmap.base) +
                               row_id * static_cast<std::uint64_t>(table->expires_mmap.width);
    std::memcpy(&ts, cell, sizeof(ts));
    return ts;
}

bool StorageEngine::create_table(const std::string& table_name,
                                 const std::vector<ColumnDef>& columns,
                                 const std::string& primary_key,
                                 std::string* err) {
    if (table_name.empty() || columns.empty()) {
        if (err) {
            *err = "invalid table definition";
        }
        return false;
    }

    std::unique_lock lock(tables_mutex_);
    if (tables_.find(table_name) != tables_.end()) {
        if (err) {
            *err = "table already exists";
        }
        return false;
    }

    std::size_t pk_index = 0;
    bool has_primary_key = false;
    if (!primary_key.empty()) {
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].name == primary_key) {
                pk_index = i;
                has_primary_key = true;
                break;
            }
        }
        if (!has_primary_key) {
            if (err) {
                *err = "primary key column not found";
            }
            return false;
        }
    } else {
        for (std::size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].is_primary_key) {
                pk_index = i;
                has_primary_key = true;
                break;
            }
        }
    }

    auto table = std::make_shared<Table>();
    table->name = table_name;
    table->columns = columns;
    table->pk_index = pk_index;
    table->has_primary_key = has_primary_key;
    table->pk_is_int = has_primary_key && (columns[pk_index].type == DataType::INT);
    table->schema_path = (fs::path(data_dir_) / "tables" / (table_name + ".schema")).string();
    table->table_dir = (fs::path(data_dir_) / "tables" / table_name).string();
    table->next_row_id.store(0, std::memory_order_release);

    if (::pthread_rwlock_init(&table->rwlock, nullptr) != 0) {
        if (err) {
            *err = "pthread_rwlock_init failed";
        }
        return false;
    }

    try {
        fs::create_directories(table->table_dir);
    } catch (const std::exception& ex) {
        if (err) {
            *err = ex.what();
        }
        return false;
    }

    std::ofstream schema_out(table->schema_path, std::ios::trunc);
    if (!schema_out) {
        if (err) {
            *err = "failed to create schema file";
        }
        return false;
    }

    schema_out << "TABLE\t" << table_name << "\n";
    if (has_primary_key) {
        schema_out << "PK\t" << columns[pk_index].name << "\n";
    } else {
        schema_out << "NOPK\n";
    }
    for (const auto& col : columns) {
        schema_out << "COL\t" << col.name << "\t" << type_to_string(col.type) << "\n";
    }

    table->columns_mmap_count = columns.size();
    table->columns_mmap = static_cast<Table::ColumnFile*>(std::calloc(table->columns_mmap_count, sizeof(Table::ColumnFile)));
    if (!table->columns_mmap) {
        if (err) {
            *err = "failed to allocate column mappings";
        }
        return false;
    }

    for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
        const std::string path = (fs::path(table->table_dir) / ("col_" + std::to_string(i) + ".bin")).string();
        if (!map_column_file(&table->columns_mmap[i], path, columns[i].type, width_for_type(columns[i].type), kDefaultInitialRows, err)) {
            return false;
        }
    }

    const std::string exp_path = (fs::path(table->table_dir) / "__expires.bin").string();
    if (!map_column_file(&table->expires_mmap, exp_path, DataType::DATETIME, 8, kDefaultInitialRows, err)) {
        return false;
    }

    if (!init_memtable(table.get(), err)) {
        return false;
    }

    table->build_column_index_cache();
    tables_[table_name] = table;
    return true;
}

bool StorageEngine::insert_row(const std::string& table_name,
                               const std::vector<std::string>& values,
                               std::int64_t expires_at,
                               std::string* err) {
    std::vector<std::string> copied = values;
    return insert_row(table_name, std::move(copied), expires_at, err);
}

bool StorageEngine::truncate_table(const std::string& table_name, std::string* err) {
    std::shared_ptr<Table> table;
    if (!get_table(table_name, &table, err)) {
        return false;
    }

    wait_for_pending_flush(table.get());
    ::pthread_rwlock_wrlock(&table->rwlock);

    table->row_count = 0;
    table->next_row_id.store(0, std::memory_order_release);
    table->primary_index.clear();
    table->primary_int_index.clear();

    auto remap_reset = [&](Table::ColumnFile* col) -> bool {
        if (!col || col->fd < 0) {
            if (err) {
                *err = "invalid column mapping during truncate";
            }
            return false;
        }

        if (col->base && col->mapped_bytes) {
            ::msync(col->base, col->mapped_bytes, MS_SYNC);
            ::munmap(col->base, col->mapped_bytes);
            col->base = nullptr;
            col->mapped_bytes = 0;
        }

        const std::uint64_t new_capacity = kDefaultInitialRows;
        const std::uint64_t bytes =
            sizeof(ColumnHeader) + new_capacity * static_cast<std::uint64_t>(col->width);

        if (::ftruncate(col->fd, static_cast<off_t>(bytes)) != 0) {
            if (err) {
                *err = "ftruncate failed during truncate/reset for " + col->path;
            }
            return false;
        }

        void* base = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, col->fd, 0);
        if (base == MAP_FAILED) {
            if (err) {
                *err = "mmap failed during truncate/reset for " + col->path;
            }
            return false;
        }

        col->base = base;
        col->mapped_bytes = bytes;
        col->capacity_rows = new_capacity;

        auto* hdr = header_ptr(col->base);
        hdr->magic = kColumnMagic;
        hdr->type = static_cast<std::uint32_t>(col->type);
        hdr->rows = 0;
        hdr->capacity_rows = new_capacity;
        hdr->width = col->width;
        hdr->reserved = 0;
        return true;
    };

    for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
        if (!remap_reset(&table->columns_mmap[i])) {
            ::pthread_rwlock_unlock(&table->rwlock);
            return false;
        }
    }

    if (!remap_reset(&table->expires_mmap)) {
        ::pthread_rwlock_unlock(&table->rwlock);
        return false;
    }

    for (int bi = 0; bi < 2; ++bi) {
        auto& buf = table->buffers[bi];
        if (!buf.initialized) {
            continue;
        }
        buf.row_count.store(0, std::memory_order_release);
        buf.data_used_bytes.store(0, std::memory_order_release);
    }

    table->active_buf.store(0, std::memory_order_release);
    table->flush_pending.store(false, std::memory_order_release);
    ::pthread_rwlock_unlock(&table->rwlock);
    return true;
}

bool StorageEngine::insert_row(const std::string& table_name,
                               std::vector<std::string>&& values,
                               std::int64_t expires_at,
                               std::string* err) {
    std::shared_ptr<Table> table;
    if (!get_table(table_name, &table, err)) {
        return false;
    }

    ::pthread_rwlock_wrlock(&table->rwlock);

    if (values.size() != table->columns.size()) {
        if (err) {
            *err = "column count mismatch";
        }
        ::pthread_rwlock_unlock(&table->rwlock);
        return false;
    }

    for (std::size_t i = 0; i < values.size(); ++i) {
        if (!validate_value(table->columns[i].type, values[i])) {
            if (err) {
                *err = "invalid value for column: " + table->columns[i].name;
            }
            ::pthread_rwlock_unlock(&table->rwlock);
            return false;
        }
    }

    if (!table->buffers[0].initialized && !init_memtable(table.get(), err)) {
        ::pthread_rwlock_unlock(&table->rwlock);
        return false;
    }

    std::int64_t pk_int_value = 0;
    if (table->has_primary_key) {
        const std::string& pk = values[table->pk_index];
        if (table->pk_is_int) {
            pk_int_value = fast_parse_int64(pk.c_str());
            if (table->primary_int_index.find(pk_int_value) != table->primary_int_index.end()) {
                if (err) *err = "duplicate primary key";
                ::pthread_rwlock_unlock(&table->rwlock);
                return false;
            }
        } else if (table->primary_index.contains(pk.c_str())) {
            if (err) *err = "duplicate primary key";
            ::pthread_rwlock_unlock(&table->rwlock);
            return false;
        }
    }

    // Get a pointer to the active buffer
    const int act = table->active_buf.load(std::memory_order_acquire);
    RawMemTableBuf* buf = &table->buffers[act];

    // If active buffer is full, swap and async flush
    if (buf->row_count.load(std::memory_order_acquire) >= buf->capacity_rows) {
        // If the OTHER buffer is still being flushed, wait synchronously
        if (table->flush_pending.load(std::memory_order_acquire)) {
            ::pthread_rwlock_unlock(&table->rwlock);
            wait_for_pending_flush(table.get());
            ::pthread_rwlock_wrlock(&table->rwlock);
        }
        // Swap active index
        const int other = 1 - act;
        table->active_buf.store(other, std::memory_order_release);
        table->flush_pending.store(true, std::memory_order_release);
        // Trigger background flush of the OLD buffer
        trigger_async_flush(table.get(), act);
        buf = &table->buffers[other];
    }

    std::uint64_t predicted_row_id = 0;
    if (!append_memtable_row_locked(table.get(), buf, values, expires_at, &predicted_row_id, err)) {
        ::pthread_rwlock_unlock(&table->rwlock);
        return false;
    }

    if (table->has_primary_key) {
        if (table->pk_is_int) {
            const std::uint32_t pending_idx = buf->row_count.load(std::memory_order_acquire) - 1;
            buf->pending_pks[pending_idx] = static_cast<std::uint64_t>(pk_int_value);
            table->primary_int_index[pk_int_value] = static_cast<std::size_t>(predicted_row_id);
        } else {
            table->primary_index.put(values[table->pk_index].c_str(), predicted_row_id);
        }
    }

    const bool became_row_full = buf->row_count.load(std::memory_order_acquire) >= buf->capacity_rows;
    const bool became_byte_full = buf->data_used_bytes.load(std::memory_order_acquire) >= buf->data_capacity_bytes;
    if ((became_row_full || became_byte_full) && !table->flush_pending.load(std::memory_order_acquire)) {
        const int now_active = table->active_buf.load(std::memory_order_acquire);
        if (now_active == act) {
            const int other = 1 - act;
            table->active_buf.store(other, std::memory_order_release);
            table->flush_pending.store(true, std::memory_order_release);
            trigger_async_flush(table.get(), act);
        }
    }

    ::pthread_rwlock_unlock(&table->rwlock);
    return true;
}

bool StorageEngine::append_memtable_row_fast_locked(Table* table,
                                                    RawMemTableBuf* buf,
                                                    std::uint64_t needed_bytes,
                                                    std::int64_t expires_at,
                                                    std::uint64_t* predicted_row_id,
                                                    std::uint32_t* pending_idx,
                                                    std::uint64_t* row_offset,
                                                    std::string* err) {
    if (!table || !buf || !buf->initialized) {
        if (err) *err = "memtable is not initialized";
        return false;
    }
    const std::uint32_t slot = buf->row_count.load(std::memory_order_acquire);
    if (slot >= buf->capacity_rows) {
        if (err) *err = "memtable is full";
        return false;
    }

    const std::uint64_t used_bytes = buf->data_used_bytes.load(std::memory_order_acquire);
    if (used_bytes + needed_bytes > buf->data_capacity_bytes) {
        std::uint64_t next_capacity = std::max<std::uint64_t>(buf->data_capacity_bytes, 1);
        while (next_capacity < used_bytes + needed_bytes) {
            next_capacity *= 2;
        }
        char* next_data = static_cast<char*>(
            std::realloc(buf->data, static_cast<std::size_t>(next_capacity)));
        if (!next_data) {
            if (err) *err = "failed to grow memtable arena";
            return false;
        }
        buf->data = next_data;
        buf->data_capacity_bytes = next_capacity;
    }

    const std::uint64_t row_id = table->next_row_id.fetch_add(1, std::memory_order_acq_rel);
    const std::uint64_t start = used_bytes;
    const std::uint64_t end = start + needed_bytes;

    buf->data_used_bytes.store(end, std::memory_order_release);
    buf->row_offsets[slot] = start;
    buf->row_lengths[slot] = static_cast<std::uint32_t>(needed_bytes);
    buf->row_ids[slot] = row_id;
    buf->expires_at[slot] = expires_at;
    buf->row_count.store(slot + 1, std::memory_order_release);

    if (predicted_row_id) {
        *predicted_row_id = row_id;
    }
    if (pending_idx) {
        *pending_idx = slot;
    }
    if (row_offset) {
        *row_offset = start;
    }
    return true;
}


bool StorageEngine::insert_row_fast(const std::string& table_name,
                                    const ArenaStringView* values,
                                    std::uint32_t value_count,
                                    std::int64_t expires_at,
                                    std::string* err) {
    return insert_rows_fast(table_name, values, value_count, 1, expires_at, err);
}

bool StorageEngine::insert_rows_fast(const std::string& table_name,
                                     const ArenaStringView* values,
                                     std::uint32_t value_count,
                                     std::uint32_t row_count,
                                     std::int64_t expires_at,
                                     std::string* err) {
    ScopedProfile prof_total(&g_prof_insert_rows_fast);
    std::shared_ptr<Table> table;
    if (!get_table(table_name, &table, err)) {
        return false;
    }

    if (row_count == 0) {
        return true;
    }

    if (!values) {
        if (err) {
            *err = "values buffer is null";
        }
        return false;
    }

    if (value_count != table->columns.size()) {
        if (err) {
            *err = "column count mismatch";
        }
        return false;
    }

    static const bool kTrustInsertValues = []() {
        const char* env = std::getenv("FLEXQL_TRUST_INSERT_VALUES");
        return env && *env != '\0' && std::strcmp(env, "0") != 0;
    }();

    const std::size_t ncols = static_cast<std::size_t>(value_count);
    const bool use_pk = table->has_primary_key;
    std::vector<std::uint64_t> row_needed_bytes(row_count, 0);
    std::vector<std::int64_t> pk_int_values;
    std::vector<std::string> pk_strings;
    if (use_pk) {
        if (table->pk_is_int) {
            pk_int_values.resize(row_count, 0);
        } else {
            pk_strings.resize(row_count);
        }
    }

    for (std::uint32_t row = 0; row < row_count; ++row) {
        const ArenaStringView* row_values = values + static_cast<std::size_t>(row) * ncols;

        if (use_pk) {
            if (table->pk_is_int) {
                if (!parse_int_view(row_values[table->pk_index], &pk_int_values[row])) {
                    if (err) {
                        *err = "invalid value for column: " + table->columns[table->pk_index].name;
                    }
                    return false;
                }
            } else {
                const ArenaStringView& pk = row_values[table->pk_index];
                if (pk.data) {
                    pk_strings[row].assign(pk.data, pk.length);
                }
            }
        }

        std::uint64_t needed_bytes = 0;
        for (std::size_t col = 0; col < ncols; ++col) {
            const ArenaStringView& value = row_values[col];
            if (!value.data && value.length > 0) {
                if (err) {
                    *err = "invalid value pointer";
                }
                return false;
            }

            if (!kTrustInsertValues) {
                switch (table->columns[col].type) {
                    case DataType::INT: {
                        if (!parse_int_view(value, nullptr)) {
                            if (err) {
                                *err = "invalid value for column: " + table->columns[col].name;
                            }
                            return false;
                        }
                        break;
                    }
                    case DataType::DECIMAL: {
                        const char* p = value.data;
                        std::size_t len = value.length;
                        if (len == 0) {
                            if (err) {
                                *err = "invalid decimal";
                            }
                            return false;
                        }
                        if (*p == '-' || *p == '+') {
                            ++p;
                            --len;
                        }
                        if (len == 0) {
                            if (err) {
                                *err = "invalid decimal";
                            }
                            return false;
                        }
                        bool has_dot = false;
                        for (std::size_t i = 0; i < len; ++i) {
                            if (p[i] == '.') {
                                if (has_dot) {
                                    if (err) {
                                        *err = "invalid decimal";
                                    }
                                    return false;
                                }
                                has_dot = true;
                            } else if (p[i] < '0' || p[i] > '9') {
                                if (err) {
                                    *err = "invalid decimal characters";
                                }
                                return false;
                            }
                        }
                        break;
                    }
                    case DataType::VARCHAR:
                    case DataType::DATETIME:
                        break;
                }
            }

            const std::uint16_t n = static_cast<std::uint16_t>(std::min<std::size_t>(value.length, 65535));
            needed_bytes += sizeof(std::uint16_t) + static_cast<std::uint64_t>(n);
        }
        row_needed_bytes[row] = needed_bytes;
    }

    const auto lock_wait_start = std::chrono::steady_clock::now();
    ::pthread_rwlock_wrlock(&table->rwlock);
    if (storage_profile_enabled()) {
        const auto lock_wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - lock_wait_start).count();
        profile_add(&g_prof_insert_rows_fast_lock_wait, static_cast<std::uint64_t>(lock_wait_ns));
    }

    if (!table->buffers[0].initialized && !init_memtable(table.get(), err)) {
        ::pthread_rwlock_unlock(&table->rwlock);
        return false;
    }

    for (std::uint32_t row = 0; row < row_count; ++row) {
        const ArenaStringView* row_values = values + static_cast<std::size_t>(row) * ncols;
        const std::uint64_t needed_bytes = row_needed_bytes[row];

        while (true) {
            if (use_pk) {
                if (table->pk_is_int) {
                    const std::int64_t pk_int_value = pk_int_values[row];
                    if (table->primary_int_index.find(pk_int_value) != table->primary_int_index.end()) {
                        if (err) {
                            *err = "duplicate primary key";
                        }
                        ::pthread_rwlock_unlock(&table->rwlock);
                        return false;
                    }
                } else {
                    if (table->primary_index.contains(pk_strings[row].c_str())) {
                        if (err) {
                            *err = "duplicate primary key";
                        }
                        ::pthread_rwlock_unlock(&table->rwlock);
                        return false;
                    }
                }
            }

            int act = table->active_buf.load(std::memory_order_acquire);
            RawMemTableBuf* buf = &table->buffers[act];

            const bool row_full = buf->row_count.load(std::memory_order_acquire) >= buf->capacity_rows;
            const bool byte_full =
                buf->data_used_bytes.load(std::memory_order_acquire) + needed_bytes > buf->data_capacity_bytes;
            if (row_full || byte_full) {
                if (table->flush_pending.load(std::memory_order_acquire)) {
                    ::pthread_rwlock_unlock(&table->rwlock);
                    wait_for_pending_flush(table.get());
                    const auto relock_wait_start = std::chrono::steady_clock::now();
                    ::pthread_rwlock_wrlock(&table->rwlock);
                    if (storage_profile_enabled()) {
                        const auto lock_wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - relock_wait_start).count();
                        profile_add(&g_prof_insert_rows_fast_lock_wait, static_cast<std::uint64_t>(lock_wait_ns));
                    }
                    continue;
                }

                const int other = 1 - act;
                table->active_buf.store(other, std::memory_order_release);
                table->flush_pending.store(true, std::memory_order_release);
                trigger_async_flush(table.get(), act);
                act = other;
                buf = &table->buffers[other];
            }

            std::uint64_t predicted_row_id = 0;
            std::uint32_t pending_idx = 0;
            std::uint64_t row_offset = 0;
            if (!append_memtable_row_fast_locked(table.get(),
                                                 buf,
                                                 needed_bytes,
                                                 expires_at,
                                                 &predicted_row_id,
                                                 &pending_idx,
                                                 &row_offset,
                                                 err)) {
                ::pthread_rwlock_unlock(&table->rwlock);
                return false;
            }

            char* row_write_ptr = buf->data + row_offset;
            for (std::size_t col = 0; col < ncols; ++col) {
                const ArenaStringView& value = row_values[col];
                const std::uint16_t n = static_cast<std::uint16_t>(std::min<std::size_t>(value.length, 65535));
                std::memcpy(row_write_ptr, &n, sizeof(n));
                row_write_ptr += sizeof(n);
                if (n > 0) {
                    std::memcpy(row_write_ptr, value.data, n);
                    row_write_ptr += n;
                }
            }

            if (use_pk) {
                if (table->pk_is_int) {
                    const std::int64_t pk_int_value = pk_int_values[row];
                    buf->pending_pks[pending_idx] = static_cast<std::uint64_t>(pk_int_value);
                    table->primary_int_index[pk_int_value] = static_cast<std::size_t>(predicted_row_id);
                } else {
                    table->primary_index.put(pk_strings[row].c_str(), predicted_row_id);
                }
            }

            const bool became_row_full = buf->row_count.load(std::memory_order_acquire) >= buf->capacity_rows;
            const bool became_byte_full =
                buf->data_used_bytes.load(std::memory_order_acquire) >= buf->data_capacity_bytes;
            if ((became_row_full || became_byte_full) && !table->flush_pending.load(std::memory_order_acquire)) {
                const int now_active = table->active_buf.load(std::memory_order_acquire);
                if (now_active == act) {
                    const int other = 1 - act;
                    table->active_buf.store(other, std::memory_order_release);
                    table->flush_pending.store(true, std::memory_order_release);
                    trigger_async_flush(table.get(), act);
                }
            }

            break;
        }
    }

    ::pthread_rwlock_unlock(&table->rwlock);
    return true;
}

// bool StorageEngine::insert_row_fast(const std::string& table_name,
//                                     const ArenaStringView* values,
//                                     std::uint32_t value_count,
//                                     std::int64_t expires_at,
//                                     std::string* err) {
//     std::shared_ptr<Table> table;
//     if (!get_table(table_name, &table, err)) {
//         return false;
//     }

//     while (table->spinlock.test_and_set(std::memory_order_acquire)) {
//     }

//     if (value_count != table->columns.size()) {
//         if (err) *err = "column count mismatch";
//         table->spinlock.clear(std::memory_order_release);
//         return false;
//     }

//     // Lightweight validation using ArenaStringView
//     for (std::size_t i = 0; i < value_count; ++i) {
//         switch (table->columns[i].type) {
//             case DataType::INT: {
//                 if (!fast_parse_int64_strict(values[i].data, nullptr)) {
//                     if (err) *err = "invalid value for column: " + table->columns[i].name;
//                     table->spinlock.clear(std::memory_order_release);
//                     return false;
//                 }
//                 break;
//             }
//             case DataType::DECIMAL: {
//                 char* ep = nullptr;
//                 (void)std::strtod(values[i].data, &ep);
//                 if (ep == values[i].data) {
//                     if (err) *err = "invalid value for column: " + table->columns[i].name;
//                     table->spinlock.clear(std::memory_order_release);
//                     return false;
//                 }
//                 break;
//             }
//             case DataType::VARCHAR:
//             case DataType::DATETIME:
//                 break;
//         }
//     }

//     if (!table->buffers[0].initialized && !init_memtable(table.get(), err)) {
//         table->spinlock.clear(std::memory_order_release);
//         return false;
//     }

//     const ArenaStringView& pk = values[table->pk_index];
//     std::int64_t pk_int_value = 0;
//     if (table->pk_is_int) {
//         pk_int_value = fast_parse_int64(pk.data);
//         if (table->primary_int_index.find(pk_int_value) != table->primary_int_index.end()) {
//             if (err) *err = "duplicate primary key";
//             table->spinlock.clear(std::memory_order_release);
//             return false;
//         }
//     } else {
//         if (table->primary_index.contains(pk.data)) {
//             if (err) *err = "duplicate primary key";
//             table->spinlock.clear(std::memory_order_release);
//             return false;
//         }
//     }

//     // Get active buffer
//     int act = table->active_buf.load(std::memory_order_acquire);
//     RawMemTableBuf* buf = &table->buffers[act];

//     // If active buffer is full, swap and async flush
//     if (buf->row_count >= buf->capacity_rows) {
//         if (table->flush_pending.load(std::memory_order_acquire)) {
//             table->spinlock.clear(std::memory_order_release);
//             wait_for_pending_flush(table.get());
//             while (table->spinlock.test_and_set(std::memory_order_acquire)) {
//             }
//             act = table->active_buf.load(std::memory_order_acquire);
//             buf = &table->buffers[act];
//         }
//         if (buf->row_count >= buf->capacity_rows) {
//             const int other = 1 - act;
//             table->active_buf.store(other, std::memory_order_release);
//             table->flush_pending.store(true, std::memory_order_release);
//             trigger_async_flush(table.get(), act);
//             buf = &table->buffers[other];
//         }
//     }

//     if (buf->row_count >= buf->capacity_rows) {
//         if (err) *err = "memtable is full";
//         table->spinlock.clear(std::memory_order_release);
//         return false;
//     }

//     std::uint64_t predicted_row_id = 0;
//     if (!append_memtable_row_fast_locked(table.get(), buf, values, value_count, expires_at, &predicted_row_id, err)) {
//         table->spinlock.clear(std::memory_order_release);
//         return false;
//     }

//     if (table->pk_is_int) {
//         const std::uint32_t pending_idx = buf->row_count - 1;
//         buf->pending_pks[pending_idx] = static_cast<std::uint64_t>(pk_int_value);
//     } else {
//         table->primary_index.put(pk.data, predicted_row_id);
//     }

//     table->spinlock.clear(std::memory_order_release);
//     return true;
// }

bool StorageEngine::scan_table(const std::string& table_name,
                               std::vector<StoredRow>* rows,
                               std::string* err) {
    if (!rows) {
        if (err) {
            *err = "rows output is null";
        }
        return false;
    }

    rows->clear();
    return scan_table_rows(
        table_name,
        [&](const StoredRow& row) {
            rows->push_back(row);
            return true;
        },
        err);
}

bool StorageEngine::stream_table_rows_chunked(
    const std::string& table_name,
    const std::vector<int>& col_indexes,
    std::size_t batch_rows,
    const std::function<bool(const char*, std::size_t, std::size_t)>& on_chunk,
    std::string* err) {
    if (!on_chunk) {
        if (err) {
            *err = "chunk callback is null";
        }
        return false;
    }
    if (col_indexes.empty()) {
        return true;
    }

    std::shared_ptr<Table> table;
    if (!get_table(table_name, &table, err)) {
        return false;
    }

    for (int idx : col_indexes) {
        if (idx < 0 || static_cast<std::size_t>(idx) >= table->columns_mmap_count) {
            if (err) {
                *err = "column index out of bounds";
            }
            return false;
        }
    }

    if (batch_rows == 0) {
        batch_rows = 1024;
    }

    ::pthread_rwlock_rdlock(&table->rwlock);

    const std::int64_t now = utils::now_epoch_seconds();
    std::vector<std::uint64_t> live_row_ids;
    live_row_ids.reserve(batch_rows);

    std::vector<const std::uint8_t*> column_cells(col_indexes.size() * batch_rows, nullptr);
    std::string chunk;
    chunk.reserve(batch_rows * col_indexes.size() * 16);

    auto append_cell = [&](const Table::ColumnFile& col, const std::uint8_t* cell, std::string& out) {
        switch (col.type) {
            case DataType::INT: {
                std::int64_t v = 0;
                std::memcpy(&v, cell, sizeof(v));
                char buf[32];
                char* end = fast_itoa(buf, v);
                out.append(buf, static_cast<std::size_t>(end - buf));
                break;
            }
            case DataType::DECIMAL: {
                double v = 0;
                std::memcpy(&v, cell, sizeof(v));
                char buf[64];
                char* end = fast_dtoa(buf, v);
                out.append(buf, static_cast<std::size_t>(end - buf));
                break;
            }
            case DataType::DATETIME: {
                std::int64_t v = 0;
                std::memcpy(&v, cell, sizeof(v));
                char buf[32];
                char* end = fast_itoa(buf, v);
                out.append(buf, static_cast<std::size_t>(end - buf));
                break;
            }
            case DataType::VARCHAR: {
                std::uint16_t n = 0;
                std::memcpy(&n, cell, sizeof(n));
                const std::size_t max_payload = col.width > 2 ? col.width - 2 : 0;
                if (n > max_payload) {
                    n = static_cast<std::uint16_t>(max_payload);
                }
                append_escaped(out, reinterpret_cast<const char*>(cell + 2), n);
                break;
            }
        }
    };

    auto flush_disk_batch = [&]() -> bool {
        if (live_row_ids.empty()) {
            return true;
        }

        const std::size_t rows = live_row_ids.size();
        for (std::size_t c = 0; c < col_indexes.size(); ++c) {
            const Table::ColumnFile& col = table->columns_mmap[static_cast<std::size_t>(col_indexes[c])];
            const std::uint64_t width = static_cast<std::uint64_t>(col.width);
            const std::uint8_t* base = data_ptr(col.base);
            const std::size_t base_idx = c * batch_rows;
            for (std::size_t r = 0; r < rows; ++r) {
                column_cells[base_idx + r] = base + live_row_ids[r] * width;
            }
        }

        chunk.clear();
        for (std::size_t r = 0; r < rows; ++r) {
            for (std::size_t c = 0; c < col_indexes.size(); ++c) {
                if (c > 0) {
                    chunk.push_back('\t');
                }
                const Table::ColumnFile& col = table->columns_mmap[static_cast<std::size_t>(col_indexes[c])];
                append_cell(col, column_cells[c * batch_rows + r], chunk);
            }
            chunk.push_back('\n');
        }

        const bool ok = on_chunk(chunk.data(), chunk.size(), rows);
        live_row_ids.clear();
        return ok;
    };

    for (std::uint64_t row_id = 0; row_id < table->row_count; ++row_id) {
        const std::int64_t expires_at = read_expires(table.get(), row_id);
        if (now >= expires_at) {
            continue;
        }

        live_row_ids.push_back(row_id);
        if (live_row_ids.size() >= batch_rows && !flush_disk_batch()) {
            ::pthread_rwlock_unlock(&table->rwlock);
            return true;
        }
    }

    if (!flush_disk_batch()) {
        ::pthread_rwlock_unlock(&table->rwlock);
        return true;
    }

    // Pending rows still in memtables are preserved for correctness.
    std::vector<int> projected_pos(table->columns_mmap_count, -1);
    for (std::size_t i = 0; i < col_indexes.size(); ++i) {
        projected_pos[static_cast<std::size_t>(col_indexes[i])] = static_cast<int>(i);
    }
    std::vector<std::string_view> pending_views(col_indexes.size());

    std::size_t pending_rows_in_chunk = 0;
    chunk.clear();
    for (int bi = 0; bi < 2; ++bi) {
        const RawMemTableBuf* buf = &table->buffers[bi];
        const std::uint32_t pending_count =
            std::min<std::uint32_t>(buf->row_count.load(std::memory_order_acquire), buf->capacity_rows);
        for (std::uint32_t pending = 0; pending < pending_count; ++pending) {
            const std::uint64_t row_id = buf->row_ids[pending];
            if (row_id < table->row_count) {
                continue;
            }
            const std::int64_t expires_at = buf->expires_at[pending];
            if (now >= expires_at) {
                continue;
            }

            std::fill(pending_views.begin(), pending_views.end(), std::string_view{});

            const std::uint64_t start = buf->row_offsets[pending];
            const std::uint64_t end = start + static_cast<std::uint64_t>(buf->row_lengths[pending]);
            const char* p = buf->data + start;
            const char* row_end = buf->data + end;

            for (std::size_t c = 0; c < table->columns_mmap_count; ++c) {
                if (p + static_cast<std::ptrdiff_t>(sizeof(std::uint16_t)) > row_end) {
                    if (err) {
                        *err = "corrupt memtable row payload";
                    }
                    ::pthread_rwlock_unlock(&table->rwlock);
                    return false;
                }

                std::uint16_t n = 0;
                std::memcpy(&n, p, sizeof(n));
                p += sizeof(n);
                if (p + n > row_end) {
                    if (err) {
                        *err = "corrupt memtable row length";
                    }
                    ::pthread_rwlock_unlock(&table->rwlock);
                    return false;
                }

                const int pos = projected_pos[c];
                if (pos >= 0) {
                    pending_views[static_cast<std::size_t>(pos)] =
                        std::string_view(p, static_cast<std::size_t>(n));
                }
                p += n;
            }

            for (std::size_t i = 0; i < col_indexes.size(); ++i) {
                if (i > 0) {
                    chunk.push_back('\t');
                }
                if (!pending_views[i].empty()) {
                    append_escaped(chunk, pending_views[i].data(), pending_views[i].size());
                }
            }
            chunk.push_back('\n');
            ++pending_rows_in_chunk;

            if (pending_rows_in_chunk >= batch_rows) {
                if (!on_chunk(chunk.data(), chunk.size(), pending_rows_in_chunk)) {
                    ::pthread_rwlock_unlock(&table->rwlock);
                    return true;
                }
                chunk.clear();
                pending_rows_in_chunk = 0;
            }
        }
    }

    if (pending_rows_in_chunk > 0) {
        if (!on_chunk(chunk.data(), chunk.size(), pending_rows_in_chunk)) {
            ::pthread_rwlock_unlock(&table->rwlock);
            return true;
        }
    }

    ::pthread_rwlock_unlock(&table->rwlock);
    return true;
}

bool StorageEngine::scan_table_rows(const std::string& table_name,
                                    const std::function<bool(const StoredRow&)>& visitor,
                                    std::string* err) {
    std::shared_ptr<Table> table;
    if (!get_table(table_name, &table, err)) {
        return false;
    }

    ::pthread_rwlock_rdlock(&table->rwlock);

    StoredRow row;
    row.values.resize(table->columns_mmap_count);

    const auto now = utils::now_epoch_seconds();
    for (std::uint64_t row_id = 0; row_id < table->row_count; ++row_id) {
        const std::int64_t expires_at = read_expires(table.get(), row_id);
        if (now >= expires_at) {
            continue;
        }

        row.row_id = row_id;
        row.expires_at = expires_at;
        for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
            (void)read_value(&table->columns_mmap[i], row_id, &row.values[i]);
        }

        if (!visitor(row)) {
            ::pthread_rwlock_unlock(&table->rwlock);
            return true;
        }
    }

    for (int bi = 0; bi < 2; ++bi) {
        const RawMemTableBuf* buf = &table->buffers[bi];
        const std::uint32_t pending_count =
            std::min<std::uint32_t>(buf->row_count.load(std::memory_order_acquire), buf->capacity_rows);
        for (std::uint32_t pending = 0; pending < pending_count; ++pending) {
            if (!decode_buf_row(table.get(), buf, pending, &row, err)) {
                ::pthread_rwlock_unlock(&table->rwlock);
                return false;
            }
            if (row.row_id < table->row_count || now >= row.expires_at) {
                continue;
            }

            if (!visitor(row)) {
                ::pthread_rwlock_unlock(&table->rwlock);
                return true;
            }
        }
    }

    ::pthread_rwlock_unlock(&table->rwlock);
    return true;
}

bool StorageEngine::get_row_by_pk(const std::string& table_name,
                                  const std::string& pk_value,
                                  StoredRow* row,
                                  std::string* err) {
    if (!row) return false;
    std::shared_ptr<Table> table;
    if (!get_table(table_name, &table, err)) return false;

    ::pthread_rwlock_rdlock(&table->rwlock);

    if (!table->has_primary_key) {
        if (err) *err = "no primary key defined";
        ::pthread_rwlock_unlock(&table->rwlock);
        return false;
    }

    std::uint64_t row_id = 0;
    bool found_in_index = false;

    // 1. Try to find the row in the flushed disk index
    if (table->pk_is_int) {
        try {
            const auto pk_int = std::stoll(pk_value);
            const auto it = table->primary_int_index.find(pk_int);
            if (it != table->primary_int_index.end()) {
                row_id = static_cast<std::uint64_t>(it->second);
                found_in_index = true;
            }
        } catch (...) {}
    } else {
        if (table->primary_index.get(pk_value.c_str(), &row_id)) {
            found_in_index = true;
        }
    }

    if (found_in_index) {
        if (row_id < table->row_count) {
            const std::int64_t expires_at = read_expires(table.get(), row_id);
            if (utils::now_epoch_seconds() >= expires_at) {
                if (err) *err = "row expired";
                ::pthread_rwlock_unlock(&table->rwlock);
                return false;
            }
            row->row_id = row_id;
            row->expires_at = expires_at;
            row->values.resize(table->columns_mmap_count);
            for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
                (void)read_value(&table->columns_mmap[i], row_id, &row->values[i]);
            }
            ::pthread_rwlock_unlock(&table->rwlock);
            return true;
        }
    }

    // 2. FALLBACK: The row is likely still sitting in the RAM MemTable!
    std::uint64_t target_pk_int = 0;
    if (table->pk_is_int) {
        try { target_pk_int = std::stoull(pk_value); } catch (...) {}
    }

    for (int bi = 0; bi < 2; ++bi) {
        const RawMemTableBuf* buf = &table->buffers[bi];
        if (!buf->initialized) continue;

        const std::uint32_t pending_count =
            std::min<std::uint32_t>(buf->row_count.load(std::memory_order_acquire), buf->capacity_rows);
        for (std::uint32_t pending = 0; pending < pending_count; ++pending) {
            bool match = false;
            if (table->pk_is_int) {
                if (buf->pending_pks[pending] == target_pk_int) match = true;
            } else {
                StoredRow temp;
                if (decode_buf_row(table.get(), buf, pending, &temp, nullptr)) {
                    if (temp.values[table->pk_index] == pk_value) match = true;
                }
            }

            if (match) {
                if (!decode_buf_row(table.get(), buf, pending, row, err)) {
                    ::pthread_rwlock_unlock(&table->rwlock);
                    return false;
                }
                ::pthread_rwlock_unlock(&table->rwlock);
                return true;
            }
        }
    }

    if (err) *err = "row not found";
    ::pthread_rwlock_unlock(&table->rwlock);
    return false;
}

// bool StorageEngine::get_row_by_pk(const std::string& table_name,
//                                   const std::string& pk_value,
//                                   StoredRow* row,
//                                   std::string* err) {
//     if (!row) {
//         if (err) {
//             *err = "row output is null";
//         }
//         return false;
//     }

//     std::shared_ptr<Table> table;
//     if (!get_table(table_name, &table, err)) {
//         return false;
//     }

//     ::pthread_rwlock_rdlock(&table->rwlock);

//     std::uint64_t row_id = 0;
//     if (table->pk_is_int) {
//         try {
//             const auto pk_int = std::stoll(pk_value);
//             const auto it = table->primary_int_index.find(pk_int);
//             if (it == table->primary_int_index.end()) {
//                 if (err) {
//                     *err = "row not found";
//                 }
//                 ::pthread_rwlock_unlock(&table->rwlock);
//                 return false;
//             }
//             row_id = static_cast<std::uint64_t>(it->second);
//         } catch (...) {
//             if (err) {
//                 *err = "invalid INT primary key value";
//             }
//             ::pthread_rwlock_unlock(&table->rwlock);
//             return false;
//         }
//     } else {
//         if (!table->primary_index.get(pk_value.c_str(), &row_id)) {
//             if (err) {
//                 *err = "row not found";
//             }
//             ::pthread_rwlock_unlock(&table->rwlock);
//             return false;
//         }
//     }

//     if (row_id < table->row_count) {
//         const std::int64_t expires_at = read_expires(table.get(), row_id);
//         if (utils::now_epoch_seconds() >= expires_at) {
//             if (err) {
//                 *err = "row expired";
//             }
//             ::pthread_rwlock_unlock(&table->rwlock);
//             return false;
//         }

//         row->row_id = row_id;
//         row->expires_at = expires_at;
//         row->values.resize(table->columns_mmap_count);
//         for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
//             (void)read_value(&table->columns_mmap[i], row_id, &row->values[i]);
//         }
//     } else {
//         bool found_pending = false;
//         for (int bi = 0; bi < 2 && !found_pending; ++bi) {
//             const RawMemTableBuf* buf = &table->buffers[bi];
//             for (std::uint32_t pending = 0; pending < buf->row_count; ++pending) {
//                 if (buf->row_ids[pending] != row_id) {
//                     continue;
//                 }
//                 if (!decode_buf_row(table.get(), buf, pending, row, err)) {
//                     ::pthread_rwlock_unlock(&table->rwlock);
//                     return false;
//                 }
//                 found_pending = true;
//                 break;
//             }
//         }

//         if (!found_pending) {
//             if (err) {
//                 *err = "index is out of sync";
//             }
//             ::pthread_rwlock_unlock(&table->rwlock);
//             return false;
//         }
//         if (utils::now_epoch_seconds() >= row->expires_at) {
//             if (err) {
//                 *err = "row expired";
//             }
//             ::pthread_rwlock_unlock(&table->rwlock);
//             return false;
//         }
//     }

//     ::pthread_rwlock_unlock(&table->rwlock);
//     return true;
// }

bool StorageEngine::with_row_by_pk(
    const std::string& table_name,
    const std::string& pk_value,
    const std::function<bool(const std::vector<std::string>&, std::int64_t)>& visitor,
    std::string* err) {
    if (!visitor) {
        if (err) {
            *err = "visitor is null";
        }
        return false;
    }

    StoredRow row;
    std::string local_err;
    if (!get_row_by_pk(table_name, pk_value, &row, &local_err)) {
        if (local_err == "row not found" || local_err == "row expired") {
            return true;
        }
        if (err) {
            *err = local_err;
        }
        return false;
    }

    return visitor(row.values, row.expires_at);
}

bool StorageEngine::get_table(const std::string& table_name,
                              std::shared_ptr<Table>* table,
                              std::string* err) const {
    if (!table) {
        if (err) {
            *err = "table output is null";
        }
        return false;
    }

    std::shared_lock lock(tables_mutex_);
    const auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        if (err) {
            *err = "table not found: " + table_name;
        }
        return false;
    }
    *table = it->second;
    return true;
}

bool StorageEngine::list_tables(std::vector<std::string>* table_names, std::string* err) const {
    if (!table_names) {
        if (err) {
            *err = "table names output is null";
        }
        return false;
    }

    table_names->clear();
    std::shared_lock lock(tables_mutex_);
    table_names->reserve(tables_.size());
    for (const auto& [name, table] : tables_) {
        (void)table;
        table_names->push_back(name);
    }
    std::sort(table_names->begin(), table_names->end());
    return true;
}

void StorageEngine::invalidate_table_cache_hint() {}

bool StorageEngine::load_table_from_schema(const std::string& schema_path, std::string* err) {
    std::ifstream in(schema_path);
    if (!in) {
        if (err) {
            *err = "failed to open schema: " + schema_path;
        }
        return false;
    }

    auto table = std::make_shared<Table>();
    table->schema_path = schema_path;
    table->table_dir = (fs::path(schema_path).parent_path() / fs::path(schema_path).stem()).string();

    std::string line;
    std::string pk_name;
    bool has_primary_key = false;
    while (std::getline(in, line)) {
        auto parts = utils::split(line, '\t');
        if (parts.empty()) {
            continue;
        }
        if (parts[0] == "TABLE" && parts.size() >= 2) {
            table->name = parts[1];
        } else if (parts[0] == "PK" && parts.size() >= 2) {
            pk_name = parts[1];
            has_primary_key = true;
        } else if (parts[0] == "NOPK") {
            has_primary_key = false;
        } else if (parts[0] == "COL" && parts.size() >= 3) {
            DataType t;
            if (!string_to_type(parts[2], &t)) {
                if (err) {
                    *err = "unknown column type in schema";
                }
                return false;
            }
            table->columns.push_back(ColumnDef{parts[1], t, false});
        }
    }

    if (table->name.empty() || table->columns.empty()) {
        if (err) {
            *err = "invalid schema file";
        }
        return false;
    }

    if (::pthread_rwlock_init(&table->rwlock, nullptr) != 0) {
        if (err) {
            *err = "pthread_rwlock_init failed";
        }
        return false;
    }

    table->has_primary_key = has_primary_key;
    if (has_primary_key) {
        bool found_pk = false;
        for (std::size_t i = 0; i < table->columns.size(); ++i) {
            if (table->columns[i].name == pk_name) {
                table->pk_index = i;
                table->columns[i].is_primary_key = true;
                found_pk = true;
                break;
            }
        }
        if (!found_pk) {
            if (err) {
                *err = "primary key column not found";
            }
            return false;
        }
        table->pk_is_int = (table->columns[table->pk_index].type == DataType::INT);
    } else {
        table->pk_index = 0;
        table->pk_is_int = false;
    }

    table->columns_mmap_count = table->columns.size();
    table->columns_mmap = static_cast<Table::ColumnFile*>(std::calloc(table->columns_mmap_count, sizeof(Table::ColumnFile)));
    if (!table->columns_mmap) {
        if (err) {
            *err = "failed to allocate column mappings";
        }
        return false;
    }

    std::uint64_t rows_min = UINT64_MAX;
    for (std::size_t i = 0; i < table->columns_mmap_count; ++i) {
        const std::string path = (fs::path(table->table_dir) / ("col_" + std::to_string(i) + ".bin")).string();
        if (!map_column_file(&table->columns_mmap[i], path, table->columns[i].type, width_for_type(table->columns[i].type), kDefaultInitialRows, err)) {
            return false;
        }
        rows_min = std::min(rows_min, header_ptr(table->columns_mmap[i].base)->rows);
    }

    const std::string exp_path = (fs::path(table->table_dir) / "__expires.bin").string();
    if (!map_column_file(&table->expires_mmap, exp_path, DataType::DATETIME, 8, kDefaultInitialRows, err)) {
        return false;
    }
    rows_min = std::min(rows_min, header_ptr(table->expires_mmap.base)->rows);
    if (rows_min == UINT64_MAX) {
        rows_min = 0;
    }
    table->row_count = rows_min;
    table->next_row_id.store(table->row_count, std::memory_order_release);

    if (table->has_primary_key) {
        for (std::uint64_t row_id = 0; row_id < table->row_count; ++row_id) {
            std::string pk;
            (void)read_value(&table->columns_mmap[table->pk_index], row_id, &pk);
            if (table->pk_is_int) {
                try {
                    table->primary_int_index[std::stoll(pk)] = static_cast<std::size_t>(row_id);
                } catch (...) {
                }
            } else {
                table->primary_index.put(pk.c_str(), row_id);
            }
        }
    }

    table->build_column_index_cache();

    if (!init_memtable(table.get(), err)) {
        return false;
    }

    std::unique_lock lock(tables_mutex_);
    tables_[table->name] = table;
    return true;
}

bool StorageEngine::validate_value(DataType type, const std::string& value) const {
    try {
        switch (type) {
            case DataType::INT:
                (void)std::stoll(value);
                return true;
            case DataType::DECIMAL:
                (void)std::stod(value);
                return true;
            case DataType::VARCHAR:
                return true;
            case DataType::DATETIME: {
                std::int64_t ts = 0;
                return utils::parse_datetime_to_epoch(value, &ts) || !value.empty();
            }
        }
    } catch (...) {
        return false;
    }
    return false;
}

std::string StorageEngine::type_to_string(DataType type) {
    switch (type) {
        case DataType::INT:
            return "INT";
        case DataType::DECIMAL:
            return "DECIMAL";
        case DataType::VARCHAR:
            return "VARCHAR";
        case DataType::DATETIME:
            return "DATETIME";
    }
    return "VARCHAR";
}

bool StorageEngine::string_to_type(const std::string& s, DataType* type) {
    if (!type) {
        return false;
    }
    const auto u = utils::to_upper(s);
    if (u == "INT") {
        *type = DataType::INT;
        return true;
    }
    if (u == "DECIMAL") {
        *type = DataType::DECIMAL;
        return true;
    }
    if (u == "VARCHAR") {
        *type = DataType::VARCHAR;
        return true;
    }
    if (u == "DATETIME") {
        *type = DataType::DATETIME;
        return true;
    }
    return false;
}

}  // namespace flexql
