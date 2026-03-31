#pragma once

#include <functional>
#include <string>
#include <string_view>

#include "cache/lru_cache.h"
#include "common/arena.h"
#include "common/types.h"
#include "parser/sql_parser.h"
#include "storage/storage_engine.h"

namespace flexql {

class QueryExecutor {
public:
    using HeaderCallback = std::function<bool(const std::vector<std::string>&)>;
    using RowCallback = std::function<bool(const std::vector<std::string>&)>;

    QueryExecutor(StorageEngine* storage, LRUCache* cache);

    ExecResult execute_sql(const std::string& sql);
    bool stream_sql_to_fd(std::string_view sql, int client_fd, std::string* send_buffer, std::string* err);
    bool stream_sql(const std::string& sql,
                    const HeaderCallback& on_header,
                    const RowCallback& on_row,
                    std::string* err);

private:
    ExecResult execute_statement(const Statement& stmt, const std::string& sql);
    ExecResult execute_create(const CreateTableStmt& stmt);
    ExecResult execute_insert(const InsertStmt& stmt);
    ExecResult execute_show_tables();
    ExecResult execute_describe_table(const DescribeTableStmt& stmt);
    ExecResult execute_select(const SelectStmt& stmt);
    bool stream_select(const SelectStmt& stmt,
                       const HeaderCallback& on_header,
                       const RowCallback& on_row,
                       std::string* err);

    // Fast INSERT path: uses thread_local arena, bypasses std::string entirely.
    ExecResult execute_insert_fast(const FastInsertStmt& stmt);

    // Attempts the fast INSERT path. Returns true if handled (result written to *out).
    bool try_fast_insert_path(std::string_view sql, ExecResult* out);

    bool evaluate_condition(const Condition& cond,
                            const std::vector<ColumnDef>& cols,
                            const std::vector<std::string>& row,
                            const std::string& table_name) const;

    bool compare_by_type(const std::string& lhs, const std::string& rhs, DataType type, OpType op) const;
    int find_column_index(const std::vector<ColumnDef>& cols,
                          const std::string& table_name,
                          const std::string& name) const;

    // Fast O(1) column lookup using table's cached index
    int find_column_index_fast(const Table* table, const std::string& name) const;

    StorageEngine* storage_;
    LRUCache* cache_;
    SQLParser parser_;
};

}  // namespace flexql
