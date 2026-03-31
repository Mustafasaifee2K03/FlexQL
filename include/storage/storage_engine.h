#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include "common/arena.h"
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/types.h"
#include "index/primary_index.h"
#include "utils/time_utils.h"

namespace flexql {

struct StoredRow {
    std::uint64_t row_id{0};
    std::vector<std::string> values;
    std::int64_t expires_at{0};
};

// Compact typed value - 8 bytes for numeric types, offset+length for VARCHAR
union CompactValue {
    std::int64_t int_val;
    double decimal_val;
    std::int64_t datetime_val;  // epoch seconds
    struct {
        std::uint32_t offset;  // offset into varchar_data buffer
        std::uint32_t length;  // length of the string
    } varchar;
};

// Compact row representation - eliminates per-column heap allocations
struct CompactRow {
    std::vector<CompactValue> values;
    std::string varchar_data;
    std::int64_t expires_at{0};

    void reserve(std::size_t col_count) {
        values.reserve(col_count);
    }

    std::string get_string(std::size_t col_idx, DataType type) const {
        if (col_idx >= values.size()) {
            return "";
        }
        const auto& v = values[col_idx];
        switch (type) {
            case DataType::INT:
                return std::to_string(v.int_val);
            case DataType::DECIMAL: {
                char buf[32];
                std::snprintf(buf, sizeof(buf), "%.6g", v.decimal_val);
                return buf;
            }
            case DataType::DATETIME:
                return std::to_string(v.datetime_val);
            case DataType::VARCHAR:
                if (v.varchar.length == 0) {
                    return "";
                }
                return varchar_data.substr(v.varchar.offset, v.varchar.length);
        }
        return "";
    }
};

// Legacy InMemoryRow for compatibility during transition
struct InMemoryRow {
    std::vector<std::string> values;
    std::int64_t expires_at{0};
};

// ============================================================================
// Raw MemTable buffer (C-style, no STL in hot path)
// ============================================================================
struct RawMemTableBuf {
    char* data{nullptr};
    std::uint64_t data_capacity_bytes{0};
    std::atomic<std::uint64_t> data_used_bytes{0};
    std::uint64_t* row_offsets{nullptr};
    std::uint32_t* row_lengths{nullptr};
    std::uint64_t* row_ids{nullptr};
    std::uint64_t* pending_pks{nullptr};
    std::int64_t* expires_at{nullptr};
    std::uint32_t capacity_rows{0};
    std::atomic<std::uint32_t> row_count{0};
    bool initialized{false};
};

struct Table {
    struct ColumnFile {
        int fd{-1};
        void* base{nullptr};
        std::uint64_t mapped_bytes{0};
        std::uint64_t capacity_rows{0};
        std::uint32_t width{0};
        DataType type{DataType::VARCHAR};
        std::string path;
    };

    std::string name;
    std::vector<ColumnDef> columns;
    std::size_t pk_index{0};
    bool has_primary_key{false};
    std::string schema_path;
    std::string table_dir;
    PrimaryIndex primary_index;
    std::unordered_map<std::int64_t, std::size_t> primary_int_index;
    bool pk_is_int{false};

    std::uint64_t row_count{0};
    std::atomic<std::uint64_t> next_row_id{0};

    ColumnFile* columns_mmap{nullptr};
    std::size_t columns_mmap_count{0};
    ColumnFile expires_mmap;

    // ---- Ping-Pong Double Buffers ----
    RawMemTableBuf buffers[2];
    std::atomic<int> active_buf{0};          // Index of the buffer accepting inserts
    std::atomic<bool> flush_pending{false};   // True while background is flushing

    // Legacy alias to keep code that reads memtable compiling during transition
    // Points to the active buffer. ONLY use when holding write lock.
    RawMemTableBuf& memtable_active() { return buffers[active_buf.load(std::memory_order_acquire)]; }
    const RawMemTableBuf& memtable_active() const { return buffers[active_buf.load(std::memory_order_acquire)]; }

    // Backwards-compat reference — old code used table->memtable
    // Now references the first buffer. This is only valid during init.
    RawMemTableBuf& memtable = buffers[0];

    mutable pthread_rwlock_t rwlock;
    std::atomic_flag spinlock = ATOMIC_FLAG_INIT;

    // Column name → index cache for O(1) lookup instead of O(columns)
    std::unordered_map<std::string, std::size_t> column_index_cache;

    void build_column_index_cache() {
        column_index_cache.clear();
        column_index_cache.reserve(columns.size());
        for (std::size_t i = 0; i < columns.size(); ++i) {
            column_index_cache[columns[i].name] = i;
        }
    }

    int find_column(const std::string& name) const {
        auto it = column_index_cache.find(name);
        if (it != column_index_cache.end()) {
            return static_cast<int>(it->second);
        }
        return -1;
    }

};

class StorageEngine {
public:
    explicit StorageEngine(std::string data_dir);
    ~StorageEngine();

    bool initialize(std::string* err);
    bool create_table(const std::string& table_name,
                      const std::vector<ColumnDef>& columns,
                      const std::string& primary_key,
                      std::string* err);

    bool insert_row(const std::string& table_name,
                    const std::vector<std::string>& values,
                    std::int64_t expires_at,
                    std::string* err);

    // Move-enabled version to avoid string copies when caller is done with values
    bool insert_row(const std::string& table_name,
                    std::vector<std::string>&& values,
                    std::int64_t expires_at,
                    std::string* err);

    // Zero-copy INSERT from arena-allocated string views.
    bool insert_row_fast(const std::string& table_name,
                         const ArenaStringView* values,
                         std::uint32_t value_count,
                         std::int64_t expires_at,
                         std::string* err);

    // Batch variant for flattened row arrays: row_count * value_count values.
    bool insert_rows_fast(const std::string& table_name,
                          const ArenaStringView* values,
                          std::uint32_t value_count,
                          std::uint32_t row_count,
                          std::int64_t expires_at,
                          std::string* err);

    bool truncate_table(const std::string& table_name, std::string* err);

    bool scan_table(const std::string& table_name,
                    std::vector<StoredRow>* rows,
                    std::string* err);

    // Streams SELECT rows in row batches using column-major reads and direct text formatting.
    // Callback receives the formatted row payload bytes and row count for each emitted chunk.
    bool stream_table_rows_chunked(
        const std::string& table_name,
        const std::vector<int>& col_indexes,
        std::size_t batch_rows,
        const std::function<bool(const char*, std::size_t, std::size_t)>& on_chunk,
        std::string* err);

    // Zero-copy scan
    template <typename Visitor>
    bool scan_table_fast(const std::string& table_name,
                         Visitor&& visitor,
                         std::string* err) {
        return scan_table_rows(
            table_name,
            [&](const StoredRow& row) {
                return visitor(static_cast<std::size_t>(row.row_id), row.values, row.expires_at);
            },
            err);
    }

    // Compact scan
    template <typename Visitor>
    bool scan_table_compact(const std::string& table_name,
                            Visitor&& visitor,
                            std::string* err) {
        std::shared_ptr<Table> table;
        if (!get_table(table_name, &table, err)) {
            return false;
        }

        return scan_table_rows(
            table_name,
            [&](const StoredRow& row) {
                CompactRow compact = build_compact_row(row.values, table->columns, row.expires_at);
                return visitor(static_cast<std::size_t>(row.row_id), compact, *table);
            },
            err);
    }

    bool get_row_by_pk(const std::string& table_name,
                       const std::string& pk_value,
                       StoredRow* row,
                       std::string* err);

    bool with_row_by_pk(const std::string& table_name,
                        const std::string& pk_value,
                        const std::function<bool(const std::vector<std::string>&, std::int64_t)>& visitor,
                        std::string* err);

    bool get_table(const std::string& table_name,
                   std::shared_ptr<Table>* table,
                   std::string* err) const;

    bool list_tables(std::vector<std::string>* table_names, std::string* err) const;

    void invalidate_table_cache_hint();

private:
    static CompactRow build_compact_row(const std::vector<std::string>& values,
                                        const std::vector<ColumnDef>& columns,
                                        std::int64_t expires_at);

    bool scan_table_rows(const std::string& table_name,
                         const std::function<bool(const StoredRow&)>& visitor,
                         std::string* err);

    bool init_memtable(Table* table, std::string* err);
    bool append_memtable_row_locked(Table* table,
                                    RawMemTableBuf* buf,
                                    const std::vector<std::string>& values,
                                    std::int64_t expires_at,
                                    std::uint64_t* predicted_row_id,
                                    std::string* err);
    bool append_memtable_row_fast_locked(Table* table,
                                        RawMemTableBuf* buf,
                                        std::uint64_t needed_bytes,
                                        std::int64_t expires_at,
                                        std::uint64_t* predicted_row_id,
                                        std::uint32_t* pending_idx,
                                        std::uint64_t* row_offset,
                                        std::string* err);
    bool flush_memtable_buf_locked(Table* table, RawMemTableBuf* buf, std::string* err);
    bool flush_all_tables(std::string* err);

    // ------ Async flush ------
    void start_flush_thread();
    void stop_flush_thread();
    void trigger_async_flush(Table* table, int buf_idx);
    void wait_for_pending_flush(Table* table);

    bool map_column_file(Table::ColumnFile* col,
                         const std::string& path,
                         DataType type,
                         std::uint32_t width,
                         std::uint64_t min_capacity_rows,
                         std::string* err);
    bool grow_table_capacity(Table* table, std::uint64_t min_rows, std::string* err);
    bool write_value(Table::ColumnFile* col, std::uint64_t row_id, const std::string& value, std::string* err);
    bool read_value(const Table::ColumnFile* col, std::uint64_t row_id, std::string* out);
    std::int64_t read_expires(const Table* table, std::uint64_t row_id);
    bool write_expires(Table* table, std::uint64_t row_id, std::int64_t expires_at, std::string* err);

    bool load_table_from_schema(const std::string& schema_path, std::string* err);
    bool validate_value(DataType type, const std::string& value) const;
    static std::string type_to_string(DataType type);
    static bool string_to_type(const std::string& s, DataType* type);

    // Helper: decode a row from any RawMemTableBuf
    static bool decode_buf_row(const Table* table, const RawMemTableBuf* buf,
                               std::uint32_t pending_idx, StoredRow* out, std::string* err);

    std::size_t page_cache_limit_bytes_;
    std::size_t write_buffer_limit_bytes_;
    std::size_t page_row_count_;

    std::string data_dir_;
    mutable std::shared_mutex tables_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;

    // ---- Async flush synchronization ----
    std::mutex flush_mutex_;
    std::condition_variable flush_done_cv_;
};

}  // namespace flexql
