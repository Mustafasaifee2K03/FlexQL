#include "query/query_executor.h"

#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "network/protocol.h"
#include "utils/string_utils.h"
#include "utils/time_utils.h"

namespace flexql {

namespace {

constexpr std::size_t kArenaSize = 64 * 1024 * 1024;  // 64 MB per-thread arena

Arena& get_thread_arena() {
    static thread_local Arena arena(kArenaSize);
    return arena;
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

inline std::size_t build_result_rows_header(char* out, std::size_t rows) {
    std::memcpy(out, "RESULT_ROWS\n", 12);
    char* p = out + 12;
    p = fast_itoa(p, static_cast<std::int64_t>(rows));
    *p++ = '\n';
    return static_cast<std::size_t>(p - out);
}

inline bool ci_starts_with_insert(const char* s, std::size_t len) {
    // Check for "INSERT INTO" prefix (case-insensitive), skipping leading whitespace
    const char* p = s;
    const char* end = s + len;
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) {
        ++p;
    }
    const std::size_t remain = static_cast<std::size_t>(end - p);
    if (remain < 11) {
        return false;
    }
    return ((p[0] | 0x20) == 'i') && ((p[1] | 0x20) == 'n') && ((p[2] | 0x20) == 's') &&
           ((p[3] | 0x20) == 'e') && ((p[4] | 0x20) == 'r') && ((p[5] | 0x20) == 't') &&
           (p[6] == ' ' || p[6] == '\t') &&
           ((p[7] | 0x20) == 'i') && ((p[8] | 0x20) == 'n') && ((p[9] | 0x20) == 't') &&
           ((p[10] | 0x20) == 'o');
}

std::size_t estimate_resultset_bytes(const ResultSet& rs) {
    std::size_t bytes = 0;
    for (const auto& c : rs.column_names) {
        bytes += c.size();
    }
    for (const auto& row : rs.rows) {
        for (const auto& v : row) {
            bytes += v.size();
        }
    }
    return bytes;
}

bool should_cache_resultset(const ResultSet& rs) {
    constexpr std::size_t kMaxCacheRows = 10'000;
    constexpr std::size_t kMaxCacheBytes = 2 * 1024 * 1024;
    return rs.rows.size() <= kMaxCacheRows && estimate_resultset_bytes(rs) <= kMaxCacheBytes;
}

bool flush_send_buffer(int fd, std::string* send_buffer) {
    if (!send_buffer || send_buffer->empty()) {
        return true;
    }

    const char* buf = send_buffer->data();
    std::size_t remaining = send_buffer->size();
    while (remaining > 0) {
        const auto n = ::send(fd, buf, remaining, 0);
        if (n <= 0) {
            return false;
        }
        buf += static_cast<std::size_t>(n);
        remaining -= static_cast<std::size_t>(n);
    }

    send_buffer->clear();
    return true;
}

}  // namespace

QueryExecutor::QueryExecutor(StorageEngine* storage, LRUCache* cache)
    : storage_(storage), cache_(cache) {}

ExecResult QueryExecutor::execute_sql(const std::string& sql) {
    // Fast-path: try zero-allocation INSERT before anything else
    ExecResult fast_result;
    if (try_fast_insert_path(sql, &fast_result)) {
        if (fast_result.ok) {
            cache_->clear();
        }
        return fast_result;
    }

    const auto cache_key = utils::trim(sql);
    if (cache_key.empty()) {
        return ExecResult{false, "empty SQL", {}};
    }

    Statement stmt;
    std::string parse_err;
    if (!parser_.parse(cache_key, &stmt, &parse_err)) {
        return ExecResult{false, parse_err, {}};
    }

    if (stmt.type == StatementType::SELECT) {
        if (const auto cached = cache_->get(cache_key); cached.has_value()) {
            return ExecResult{true, "OK", cached.value()};
        }
    }

    auto result = execute_statement(stmt, cache_key);
    if (result.ok && stmt.type == StatementType::SELECT && should_cache_resultset(result.result_set)) {
        cache_->put(cache_key, result.result_set);
    }
    if (result.ok && (stmt.type == StatementType::INSERT || stmt.type == StatementType::CREATE_TABLE ||
                      stmt.type == StatementType::DELETE)) {
        cache_->clear();
    }

    return result;
}

bool QueryExecutor::stream_sql_to_fd(std::string_view sql,
                                     int client_fd,
                                     std::string* send_buffer,
                                     std::string* err) {
    ExecResult fast_result;
    if (try_fast_insert_path(sql, &fast_result)) {
        if (!fast_result.ok) {
            if (err) {
                *err = fast_result.message;
            }
            return false;
        }
        if (cache_) {
            cache_->clear();
        }
        if (send_buffer) {
            // send_buffer->append(network::serialize_ok_response());
            // return true;
            std::string payload = network::serialize_ok_response();
            std::uint32_t len = htonl(static_cast<std::uint32_t>(payload.size()));
            send_buffer->append(reinterpret_cast<const char*>(&len), sizeof(len));
            send_buffer->append(payload);
            return true;
        }
        return network::send_frame(client_fd, network::serialize_ok_response());
    }

    const std::string sql_owned(sql);
    const auto clean_sql = utils::trim(sql_owned);
    if (clean_sql.empty()) {
        if (err) {
            *err = "empty SQL";
        }
        return false;
    }

    Statement stmt;
    std::string parse_err;
    if (!parser_.parse(clean_sql, &stmt, &parse_err)) {
        if (err) {
            *err = parse_err;
        }
        return false;
    }

    const bool has_tabular_result =
        (stmt.type == StatementType::SHOW_TABLES || stmt.type == StatementType::DESCRIBE_TABLE);

    if (stmt.type != StatementType::SELECT && !has_tabular_result) {
        const auto res = execute_statement(stmt, clean_sql);
        if (!res.ok) {
            if (err) {
                *err = res.message;
            }
            return false;
        }
        if ((stmt.type == StatementType::INSERT || stmt.type == StatementType::CREATE_TABLE ||
             stmt.type == StatementType::DELETE) && cache_) {
            cache_->clear();
        }
        if (send_buffer) {
            // send_buffer->append(network::serialize_ok_response());
            // return true;

            std::string payload = network::serialize_ok_response();
            std::uint32_t len = htonl(static_cast<std::uint32_t>(payload.size()));
            send_buffer->append(reinterpret_cast<const char*>(&len), sizeof(len));
            send_buffer->append(payload);
            return true;
        }
        return network::send_frame(client_fd, network::serialize_ok_response());
    }

    if (has_tabular_result) {
        const auto res = execute_statement(stmt, clean_sql);
        if (!res.ok) {
            if (err) {
                *err = res.message;
            }
            return false;
        }

        if (!flush_send_buffer(client_fd, send_buffer)) {
            if (err) {
                *err = "failed to flush pipelined responses";
            }
            return false;
        }

        if (!network::send_frame(client_fd, network::serialize_result_start(res.result_set.column_names))) {
            if (err) {
                *err = "failed to stream result header";
            }
            return false;
        }
        if (!network::send_frame(client_fd, network::serialize_result_rows(res.result_set.rows))) {
            if (err) {
                *err = "failed to stream result rows";
            }
            return false;
        }
        if (!network::send_frame(client_fd, network::serialize_result_end())) {
            if (err) {
                *err = "failed to stream result end";
            }
            return false;
        }
        return true;
    }

    if (!flush_send_buffer(client_fd, send_buffer)) {
        if (err) {
            *err = "failed to flush pipelined responses";
        }
        return false;
    }

    const SelectStmt& s = stmt.select;
    if (s.has_join || s.has_where || s.has_order_by) {
        bool sent_header = false;
        constexpr std::size_t kFallbackBatchRows = 1000;
        std::vector<std::vector<std::string>> batch;
        batch.reserve(kFallbackBatchRows);

        auto flush_batch = [&]() -> bool {
            if (batch.empty()) {
                return true;
            }
            if (!network::send_frame(client_fd, network::serialize_result_rows(batch))) {
                return false;
            }
            batch.clear();
            return true;
        };

        std::string stream_err;
        const bool ok = stream_sql(
            clean_sql,
            [&](const std::vector<std::string>& columns) {
                sent_header = true;
                return network::send_frame(client_fd, network::serialize_result_start(columns));
            },
            [&](const std::vector<std::string>& row) {
                batch.push_back(row);
                if (batch.size() >= kFallbackBatchRows) {
                    return flush_batch();
                }
                return true;
            },
            &stream_err);

        if (!ok) {
            if (err) {
                *err = stream_err.empty() ? "query execution failed" : stream_err;
            }
            return false;
        }

        if (sent_header) {
            if (!flush_batch()) {
                if (err) {
                    *err = "failed to stream result chunk";
                }
                return false;
            }
            return network::send_frame(client_fd, network::serialize_result_end());
        }

        return network::send_frame(client_fd, network::serialize_ok_response());
    }

    std::shared_ptr<Table> table;
    std::string local_err;
    if (!storage_->get_table(s.table_name, &table, &local_err)) {
        if (err) {
            *err = local_err;
        }
        return false;
    }

    std::vector<int> col_indexes;
    std::vector<std::string> header;
    if (s.columns.empty()) {
        col_indexes.reserve(table->columns.size());
        header.reserve(table->columns.size());
        for (std::size_t i = 0; i < table->columns.size(); ++i) {
            col_indexes.push_back(static_cast<int>(i));
            header.push_back(table->columns[i].name);
        }
    } else {
        col_indexes.reserve(s.columns.size());
        header.reserve(s.columns.size());
        for (const auto& c : s.columns) {
            const int idx = find_column_index_fast(table.get(), c);
            if (idx < 0) {
                if (err) {
                    *err = "unknown column in SELECT: " + c;
                }
                return false;
            }
            col_indexes.push_back(idx);
            header.push_back(c);
        }
    }

    if (!network::send_frame(client_fd, network::serialize_result_start(header))) {
        if (err) {
            *err = "failed to stream result header";
        }
        return false;
    }

    constexpr std::size_t kBatchRows = 1024;
    if (!storage_->stream_table_rows_chunked(
            s.table_name,
            col_indexes,
            kBatchRows,
            [&](const char* payload, std::size_t payload_len, std::size_t row_count) {
                char rows_header[64];
                const std::size_t header_len = build_result_rows_header(rows_header, row_count);
                return network::send_frame_chunks(
                    client_fd,
                    rows_header,
                    header_len,
                    payload,
                    payload_len);
            },
            &local_err)) {
        if (err) {
            *err = local_err.empty() ? "failed to stream result rows" : local_err;
        }
        return false;
    }

    if (!network::send_frame(client_fd, network::serialize_result_end())) {
        if (err) {
            *err = "failed to stream result end";
        }
        return false;
    }
    return true;
}

bool QueryExecutor::stream_sql(const std::string& sql,
                              const HeaderCallback& on_header,
                              const RowCallback& on_row,
                              std::string* err) {
    // Fast-path: try zero-allocation INSERT before regex parsing
    ExecResult fast_result;
    if (try_fast_insert_path(sql, &fast_result)) {
        if (!fast_result.ok) {
            if (err) {
                *err = fast_result.message;
            }
            return false;
        }
        if (cache_) {
            cache_->clear();
        }
        return true;
    }

    const auto clean_sql = utils::trim(sql);
    if (clean_sql.empty()) {
        if (err) {
            *err = "empty SQL";
        }
        return false;
    }

    Statement stmt;
    std::string parse_err;
    if (!parser_.parse(clean_sql, &stmt, &parse_err)) {
        if (err) {
            *err = parse_err;
        }
        return false;
    }

    if (stmt.type == StatementType::SELECT) {
        return stream_select(stmt.select, on_header, on_row, err);
    }

    if (stmt.type == StatementType::SHOW_TABLES) {
        const auto res = execute_statement(stmt, clean_sql);
        if (!res.ok) {
            if (err) {
                *err = res.message;
            }
            return false;
        }
        if (on_header && !on_header(res.result_set.column_names)) {
            return false;
        }
        if (on_row) {
            for (const auto& row : res.result_set.rows) {
                if (!on_row(row)) {
                    return false;
                }
            }
        }
        return true;
    }

    if (stmt.type == StatementType::DESCRIBE_TABLE) {
        const auto res = execute_statement(stmt, clean_sql);
        if (!res.ok) {
            if (err) {
                *err = res.message;
            }
            return false;
        }
        if (on_header && !on_header(res.result_set.column_names)) {
            return false;
        }
        if (on_row) {
            for (const auto& row : res.result_set.rows) {
                if (!on_row(row)) {
                    return false;
                }
            }
        }
        return true;
    }

    const auto res = execute_statement(stmt, clean_sql);
    if (!res.ok) {
        if (err) {
            *err = res.message;
        }
        return false;
    }

    if ((stmt.type == StatementType::INSERT || stmt.type == StatementType::CREATE_TABLE ||
         stmt.type == StatementType::DELETE) && cache_) {
        cache_->clear();
    }
    return true;
}

ExecResult QueryExecutor::execute_statement(const Statement& stmt, const std::string& sql) {
    (void)sql;
    switch (stmt.type) {
        case StatementType::CREATE_TABLE:
            return execute_create(stmt.create_table);
        case StatementType::INSERT:
            return execute_insert(stmt.insert);
        case StatementType::DELETE: {
            std::string err;
            if (!storage_->truncate_table(stmt.del.table_name, &err)) {
                return ExecResult{false, err, {}};
            }
            return ExecResult{true, "OK", {}};
        }
        case StatementType::SHOW_TABLES:
            return execute_show_tables();
        case StatementType::DESCRIBE_TABLE:
            return execute_describe_table(stmt.describe);
        case StatementType::SELECT:
            return execute_select(stmt.select);
        default:
            return ExecResult{false, "unsupported statement", {}};
    }
}

ExecResult QueryExecutor::execute_create(const CreateTableStmt& stmt) {
    std::string err;
    if (!storage_->create_table(stmt.table_name, stmt.columns, stmt.primary_key, &err)) {
        return ExecResult{false, err, {}};
    }
    return ExecResult{true, "OK", {}};
}

ExecResult QueryExecutor::execute_insert(const InsertStmt& stmt) {
    std::shared_ptr<Table> table;
    std::string err;
    if (!storage_->get_table(stmt.table_name, &table, &err)) {
        return ExecResult{false, err, {}};
    }

    std::vector<std::vector<std::string>> single_row_fallback;
    const std::vector<std::vector<std::string>>* rows = &stmt.rows;
    if (rows->empty()) {
        single_row_fallback.push_back(stmt.values);
        rows = &single_row_fallback;
    }

    std::vector<int> col_map;
    if (!stmt.columns.empty()) {
        col_map.resize(stmt.columns.size(), -1);
        for (std::size_t i = 0; i < stmt.columns.size(); ++i) {
            const int col_idx = table->find_column(stmt.columns[i]);
            if (col_idx < 0) {
                return ExecResult{false, "unknown column in INSERT: " + stmt.columns[i], {}};
            }
            col_map[i] = col_idx;
        }
    }

    std::int64_t expires = 0;
    if (stmt.has_expires_at) {
        expires = stmt.expires_at;
    } else if (stmt.has_ttl) {
        expires = utils::now_epoch_seconds() + stmt.ttl_seconds;
    } else {
        // Default expiration if omitted to satisfy assignment expiration requirement.
        expires = utils::now_epoch_seconds() + 24 * 60 * 60;
    }

    for (const auto& src_values : *rows) {
        std::vector<std::string> values;
        if (!stmt.columns.empty()) {
            if (stmt.columns.size() != src_values.size()) {
                return ExecResult{false, "column count does not match values count", {}};
            }
            values.resize(table->columns.size());
            for (std::size_t i = 0; i < src_values.size(); ++i) {
                values[static_cast<std::size_t>(col_map[i])] = src_values[i];
            }
            for (std::size_t i = 0; i < values.size(); ++i) {
                if (values[i].empty()) {
                    return ExecResult{false, "missing value for column: " + table->columns[i].name, {}};
                }
            }
        } else {
            values = src_values;
        }

        if (!storage_->insert_row(stmt.table_name, std::move(values), expires, &err)) {
            return ExecResult{false, err, {}};
        }
    }
    return ExecResult{true, "OK", {}};
}

ExecResult QueryExecutor::execute_show_tables() {
    std::vector<std::string> table_names;
    std::string err;
    if (!storage_->list_tables(&table_names, &err)) {
        return ExecResult{false, err, {}};
    }

    ResultSet rs;
    rs.column_names = {"table_name"};
    rs.rows.reserve(table_names.size());
    for (const auto& name : table_names) {
        rs.rows.push_back({name});
    }
    return ExecResult{true, "OK", std::move(rs)};
}

ExecResult QueryExecutor::execute_describe_table(const DescribeTableStmt& stmt) {
    std::shared_ptr<Table> table;
    std::string err;
    if (!storage_->get_table(stmt.table_name, &table, &err)) {
        return ExecResult{false, err, {}};
    }

    auto data_type_to_string = [](DataType type) -> std::string {
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
        return "UNKNOWN";
    };

    ResultSet rs;
    rs.column_names = {"column_name", "data_type", "is_primary_key"};
    rs.rows.reserve(table->columns.size());
    for (std::size_t i = 0; i < table->columns.size(); ++i) {
        const auto& col = table->columns[i];
        const bool is_pk = table->has_primary_key && ((i == table->pk_index) || col.is_primary_key);
        rs.rows.push_back({
            col.name,
            data_type_to_string(col.type),
            is_pk ? "YES" : "NO",
        });
    }

    return ExecResult{true, "OK", std::move(rs)};
}

ExecResult QueryExecutor::execute_select(const SelectStmt& stmt) {
    std::string err;

    std::shared_ptr<Table> left_table;
    if (!storage_->get_table(stmt.table_name, &left_table, &err)) {
        return ExecResult{false, err, {}};
    }

    if (!stmt.has_join) {
        ResultSet rs;
        std::vector<int> col_indexes;

        int order_col_idx = -1;
        DataType order_col_type = DataType::VARCHAR;
        if (stmt.has_order_by) {
            order_col_idx = find_column_index_fast(left_table.get(), stmt.order_by_column);
            if (order_col_idx < 0) {
                return ExecResult{false, "unknown column in ORDER BY: " + stmt.order_by_column, {}};
            }
            order_col_type = left_table->columns[static_cast<std::size_t>(order_col_idx)].type;
        }

        std::vector<std::pair<std::string, std::vector<std::string>>> rows_with_order;

        // Build column projection indexes using cached lookup
        if (stmt.columns.empty()) {
            col_indexes.reserve(left_table->columns.size());
            rs.column_names.reserve(left_table->columns.size());
            for (std::size_t i = 0; i < left_table->columns.size(); ++i) {
                col_indexes.push_back(static_cast<int>(i));
                rs.column_names.push_back(left_table->columns[i].name);
            }
        } else {
            col_indexes.reserve(stmt.columns.size());
            rs.column_names.reserve(stmt.columns.size());
            for (const auto& c : stmt.columns) {
                const int idx = find_column_index_fast(left_table.get(), c);
                if (idx < 0) {
                    return ExecResult{false, "unknown column in SELECT: " + c, {}};
                }
                col_indexes.push_back(idx);
                rs.column_names.push_back(c);
            }
        }

        // Pre-compute WHERE column index if needed
        int where_col_idx = -1;
        if (stmt.has_where) {
            where_col_idx = find_column_index_fast(left_table.get(), stmt.where.column);
            if (where_col_idx < 0) {
                return ExecResult{false, "unknown column in WHERE: " + stmt.where.column, {}};
            }
        }

        // Check if WHERE targets primary key for fast path
        const bool pk_lookup = stmt.has_where && stmt.where.op == OpType::EQ && left_table->has_primary_key &&
                       where_col_idx == static_cast<int>(left_table->pk_index);

        if (pk_lookup) {
            StoredRow one;
            if (storage_->get_row_by_pk(stmt.table_name, stmt.where.value, &one, &err)) {
                std::vector<std::string> out;
                out.reserve(col_indexes.size());
                for (int idx : col_indexes) {
                    out.push_back(one.values[static_cast<std::size_t>(idx)]);
                }
                if (stmt.has_order_by) {
                    rows_with_order.push_back({one.values[static_cast<std::size_t>(order_col_idx)], std::move(out)});
                } else {
                    rs.rows.push_back(std::move(out));
                }
            }
        } else {
            // Use zero-copy fast scan
            rs.rows.reserve(1024);  // Pre-allocate some rows
            storage_->scan_table_fast(stmt.table_name,
                [&](std::size_t /*row_id*/, const std::vector<std::string>& values, std::int64_t /*expires_at*/) {
                    // Evaluate WHERE condition directly
                    if (stmt.has_where &&
                        !compare_by_type(values[static_cast<std::size_t>(where_col_idx)],
                                         stmt.where.value,
                                         left_table->columns[static_cast<std::size_t>(where_col_idx)].type,
                                         stmt.where.op)) {
                        return true;  // Continue to next row
                    }

                    std::vector<std::string> out;
                    out.reserve(col_indexes.size());
                    for (int idx : col_indexes) {
                        out.push_back(values[static_cast<std::size_t>(idx)]);
                    }
                    if (stmt.has_order_by) {
                        rows_with_order.push_back({values[static_cast<std::size_t>(order_col_idx)], std::move(out)});
                    } else {
                        rs.rows.push_back(std::move(out));
                    }
                    return true;  // Continue iteration
                },
                &err);
        }

        if (stmt.has_order_by) {
            auto typed_less = [&](const std::string& a, const std::string& b) {
                try {
                    switch (order_col_type) {
                        case DataType::INT:
                            return std::stoll(a) < std::stoll(b);
                        case DataType::DECIMAL:
                            return std::stod(a) < std::stod(b);
                        case DataType::DATETIME: {
                            std::int64_t ta = 0;
                            std::int64_t tb = 0;
                            const bool a_ok = utils::parse_datetime_to_epoch(a, &ta);
                            const bool b_ok = utils::parse_datetime_to_epoch(b, &tb);
                            if (a_ok && b_ok) {
                                return ta < tb;
                            }
                            return a < b;
                        }
                        case DataType::VARCHAR:
                            return a < b;
                    }
                } catch (...) {
                    return a < b;
                }
                return a < b;
            };

            std::sort(rows_with_order.begin(), rows_with_order.end(),
                      [&](const auto& lhs, const auto& rhs) {
                          if (stmt.order_by_desc) {
                              return typed_less(rhs.first, lhs.first);
                          }
                          return typed_less(lhs.first, rhs.first);
                      });

            rs.rows.reserve(rows_with_order.size());
            for (auto& entry : rows_with_order) {
                rs.rows.push_back(std::move(entry.second));
            }
        }

        return ExecResult{true, "OK", std::move(rs)};
    }

    // JOIN path
    std::shared_ptr<Table> right_table;
    if (!storage_->get_table(stmt.join.right_table, &right_table, &err)) {
        return ExecResult{false, err, {}};
    }

    int left_join_idx = find_column_index_fast(left_table.get(), stmt.join.left_expr);
    int right_join_idx = find_column_index_fast(right_table.get(), stmt.join.right_expr);
    if (left_join_idx < 0 || right_join_idx < 0) {
        return ExecResult{false, "invalid JOIN ON columns", {}};
    }

    ResultSet rs;
    bool all_cols = stmt.columns.empty();
    rs.rows.reserve(1024);

    if (all_cols) {
        rs.column_names.reserve(left_table->columns.size() + right_table->columns.size());
        for (const auto& c : left_table->columns) {
            rs.column_names.push_back(left_table->name + "." + c.name);
        }
        for (const auto& c : right_table->columns) {
            rs.column_names.push_back(right_table->name + "." + c.name);
        }
    } else {
        rs.column_names = stmt.columns;
    }

    // Pre-compute projection indexes for non-all columns
    std::vector<std::pair<int, int>> proj_indexes;  // (table_id: 0=left, 1=right, col_idx)
    if (!all_cols) {
        proj_indexes.reserve(stmt.columns.size());
        for (const auto& c : stmt.columns) {
            const int li = find_column_index_fast(left_table.get(), c);
            if (li >= 0) {
                proj_indexes.push_back({0, li});
                continue;
            }
            const int ri = find_column_index_fast(right_table.get(), c);
            if (ri >= 0) {
                proj_indexes.push_back({1, ri});
                continue;
            }
            return ExecResult{false, "unknown column in SELECT: " + c, {}};
        }
    }

    // Pre-compute WHERE column index for JOIN
    int where_col_idx_left = -1;
    int where_col_idx_right = -1;
    if (stmt.has_where) {
        where_col_idx_left = find_column_index_fast(left_table.get(), stmt.where.column);
        where_col_idx_right = find_column_index_fast(right_table.get(), stmt.where.column);
    }

    auto emit_join_row = [&](const std::vector<std::string>& l_values,
                             const std::vector<std::string>& r_values) -> bool {
        // Evaluate WHERE without merging vectors
        if (stmt.has_where) {
            bool match = false;
            if (where_col_idx_left >= 0) {
                match = compare_by_type(
                    l_values[static_cast<std::size_t>(where_col_idx_left)],
                    stmt.where.value,
                    left_table->columns[static_cast<std::size_t>(where_col_idx_left)].type,
                    stmt.where.op);
            } else if (where_col_idx_right >= 0) {
                match = compare_by_type(
                    r_values[static_cast<std::size_t>(where_col_idx_right)],
                    stmt.where.value,
                    right_table->columns[static_cast<std::size_t>(where_col_idx_right)].type,
                    stmt.where.op);
            }
            if (!match) {
                return true;  // Skip this row but continue
            }
        }

        std::vector<std::string> out;
        if (all_cols) {
            out.reserve(l_values.size() + r_values.size());
            out.insert(out.end(), l_values.begin(), l_values.end());
            out.insert(out.end(), r_values.begin(), r_values.end());
        } else {
            out.reserve(proj_indexes.size());
            for (const auto& [table_id, idx] : proj_indexes) {
                if (table_id == 0) {
                    out.push_back(l_values[static_cast<std::size_t>(idx)]);
                } else {
                    out.push_back(r_values[static_cast<std::size_t>(idx)]);
                }
            }
        }
        rs.rows.push_back(std::move(out));
        return true;
    };

    // Predicate pushdown for JOIN+WHERE when WHERE targets a primary key.
    if (stmt.has_where && stmt.where.op == OpType::EQ) {
        if (left_table->has_primary_key && where_col_idx_left == static_cast<int>(left_table->pk_index)) {
            StoredRow left_row;
            std::string lookup_err;
            if (!storage_->get_row_by_pk(left_table->name, stmt.where.value, &left_row, &lookup_err)) {
                if (lookup_err == "row not found" || lookup_err == "row expired") {
                    return ExecResult{true, "OK", std::move(rs)};
                }
                return ExecResult{false, lookup_err, {}};
            }

            if (right_table->has_primary_key && right_join_idx == static_cast<int>(right_table->pk_index)) {
                if (!storage_->with_row_by_pk(
                        right_table->name,
                        left_row.values[static_cast<std::size_t>(left_join_idx)],
                        [&](const std::vector<std::string>& r_values, std::int64_t) {
                            return emit_join_row(left_row.values, r_values);
                        },
                        &lookup_err)) {
                    return ExecResult{false, lookup_err, {}};
                }
                return ExecResult{true, "OK", std::move(rs)};
            }

            if (!storage_->scan_table_fast(
                    right_table->name,
                    [&](std::size_t, const std::vector<std::string>& r_values, std::int64_t) {
                        if (r_values[static_cast<std::size_t>(right_join_idx)] !=
                            left_row.values[static_cast<std::size_t>(left_join_idx)]) {
                            return true;
                        }
                        return emit_join_row(left_row.values, r_values);
                    },
                    &lookup_err)) {
                return ExecResult{false, lookup_err, {}};
            }
            return ExecResult{true, "OK", std::move(rs)};
        }

        if (right_table->has_primary_key && where_col_idx_right == static_cast<int>(right_table->pk_index)) {
            StoredRow right_row;
            std::string lookup_err;
            if (!storage_->get_row_by_pk(right_table->name, stmt.where.value, &right_row, &lookup_err)) {
                if (lookup_err == "row not found" || lookup_err == "row expired") {
                    return ExecResult{true, "OK", std::move(rs)};
                }
                return ExecResult{false, lookup_err, {}};
            }

            if (left_table->has_primary_key && left_join_idx == static_cast<int>(left_table->pk_index)) {
                if (!storage_->with_row_by_pk(
                        left_table->name,
                        right_row.values[static_cast<std::size_t>(right_join_idx)],
                        [&](const std::vector<std::string>& l_values, std::int64_t) {
                            return emit_join_row(l_values, right_row.values);
                        },
                        &lookup_err)) {
                    return ExecResult{false, lookup_err, {}};
                }
                return ExecResult{true, "OK", std::move(rs)};
            }

            if (!storage_->scan_table_fast(
                    left_table->name,
                    [&](std::size_t, const std::vector<std::string>& l_values, std::int64_t) {
                        if (l_values[static_cast<std::size_t>(left_join_idx)] !=
                            right_row.values[static_cast<std::size_t>(right_join_idx)]) {
                            return true;
                        }
                        return emit_join_row(l_values, right_row.values);
                    },
                    &lookup_err)) {
                return ExecResult{false, lookup_err, {}};
            }
            return ExecResult{true, "OK", std::move(rs)};
        }
    }

    // Use index-assisted join when either side joins on its primary key
    if (left_table->has_primary_key && left_join_idx == static_cast<int>(left_table->pk_index)) {
        // Scan right table, lookup left by PK
        if (!storage_->scan_table_fast(right_table->name,
            [&](std::size_t, const std::vector<std::string>& r_values, std::int64_t) {
                std::string lookup_err;
                if (!storage_->with_row_by_pk(
                        left_table->name,
                        r_values[static_cast<std::size_t>(right_join_idx)],
                        [&](const std::vector<std::string>& l_values, std::int64_t) {
                            return emit_join_row(l_values, r_values);
                        },
                        &lookup_err)) {
                    if (!lookup_err.empty()) {
                        err = lookup_err;
                    }
                    return false;
                }
                return true;
            },
            &err)) {
            return ExecResult{false, err, {}};
        }
        return ExecResult{true, "OK", std::move(rs)};
    }

    if (right_table->has_primary_key && right_join_idx == static_cast<int>(right_table->pk_index)) {
        // Scan left table, lookup right by PK
        if (!storage_->scan_table_fast(left_table->name,
            [&](std::size_t, const std::vector<std::string>& l_values, std::int64_t) {
                std::string lookup_err;
                if (!storage_->with_row_by_pk(
                        right_table->name,
                        l_values[static_cast<std::size_t>(left_join_idx)],
                        [&](const std::vector<std::string>& r_values, std::int64_t) {
                            return emit_join_row(l_values, r_values);
                        },
                        &lookup_err)) {
                    if (!lookup_err.empty()) {
                        err = lookup_err;
                    }
                    return false;
                }
                return true;
            },
            &err)) {
            return ExecResult{false, err, {}};
        }
        return ExecResult{true, "OK", std::move(rs)};
    }

    return ExecResult{false,
                      "JOIN requires one side of ON clause to be the primary key (index-based JOIN only)",
                      {}};
}

bool QueryExecutor::stream_select(const SelectStmt& stmt,
                                  const HeaderCallback& on_header,
                                  const RowCallback& on_row,
                                  std::string* err) {
    if (stmt.has_order_by) {
        const auto res = execute_select(stmt);
        if (!res.ok) {
            if (err) {
                *err = res.message;
            }
            return false;
        }
        if (on_header && !on_header(res.result_set.column_names)) {
            return false;
        }
        if (on_row) {
            for (const auto& row : res.result_set.rows) {
                if (!on_row(row)) {
                    return false;
                }
            }
        }
        return true;
    }

    std::string local_err;

    std::shared_ptr<Table> left_table;
    if (!storage_->get_table(stmt.table_name, &left_table, &local_err)) {
        if (err) {
            *err = local_err;
        }
        return false;
    }

    if (!stmt.has_join) {
        std::vector<int> col_indexes;
        std::vector<std::string> header;

        if (stmt.columns.empty()) {
            col_indexes.reserve(left_table->columns.size());
            header.reserve(left_table->columns.size());
            for (std::size_t i = 0; i < left_table->columns.size(); ++i) {
                col_indexes.push_back(static_cast<int>(i));
                header.push_back(left_table->columns[i].name);
            }
        } else {
            col_indexes.reserve(stmt.columns.size());
            header.reserve(stmt.columns.size());
            for (const auto& c : stmt.columns) {
                const int idx = find_column_index_fast(left_table.get(), c);
                if (idx < 0) {
                    if (err) {
                        *err = "unknown column in SELECT: " + c;
                    }
                    return false;
                }
                col_indexes.push_back(idx);
                header.push_back(c);
            }
        }

        if (on_header && !on_header(header)) {
            return false;
        }

        int where_col_idx = -1;
        if (stmt.has_where) {
            where_col_idx = find_column_index_fast(left_table.get(), stmt.where.column);
            if (where_col_idx < 0) {
                if (err) {
                    *err = "unknown column in WHERE: " + stmt.where.column;
                }
                return false;
            }
        }

        const bool pk_lookup = stmt.has_where && stmt.where.op == OpType::EQ && left_table->has_primary_key &&
                       where_col_idx == static_cast<int>(left_table->pk_index);

        if (pk_lookup) {
            StoredRow one;
            std::string one_err;
            if (!storage_->get_row_by_pk(stmt.table_name, stmt.where.value, &one, &one_err)) {
                if (one_err == "row not found" || one_err == "row expired") {
                    return true;
                }
                if (err) {
                    *err = one_err;
                }
                return false;
            }

            std::vector<std::string> out;
            out.reserve(col_indexes.size());
            for (int idx : col_indexes) {
                out.push_back(one.values[static_cast<std::size_t>(idx)]);
            }
            if (on_row && !on_row(out)) {
                return false;
            }
            return true;
        }

        if (!storage_->scan_table_fast(
                stmt.table_name,
                [&](std::size_t, const std::vector<std::string>& values, std::int64_t) {
                    if (stmt.has_where &&
                        !compare_by_type(values[static_cast<std::size_t>(where_col_idx)],
                                         stmt.where.value,
                                         left_table->columns[static_cast<std::size_t>(where_col_idx)].type,
                                         stmt.where.op)) {
                        return true;
                    }

                    std::vector<std::string> out;
                    out.reserve(col_indexes.size());
                    for (int idx : col_indexes) {
                        out.push_back(values[static_cast<std::size_t>(idx)]);
                    }

                    if (on_row) {
                        return on_row(out);
                    }
                    return true;
                },
                &local_err)) {
            if (err) {
                *err = local_err;
            }
            return false;
        }
        return true;
    }

    std::shared_ptr<Table> right_table;
    if (!storage_->get_table(stmt.join.right_table, &right_table, &local_err)) {
        if (err) {
            *err = local_err;
        }
        return false;
    }

    const int left_join_idx = find_column_index_fast(left_table.get(), stmt.join.left_expr);
    const int right_join_idx = find_column_index_fast(right_table.get(), stmt.join.right_expr);
    if (left_join_idx < 0 || right_join_idx < 0) {
        if (err) {
            *err = "invalid JOIN ON columns";
        }
        return false;
    }

    bool all_cols = stmt.columns.empty();
    std::vector<std::string> header;
    if (all_cols) {
        header.reserve(left_table->columns.size() + right_table->columns.size());
        for (const auto& c : left_table->columns) {
            header.push_back(left_table->name + "." + c.name);
        }
        for (const auto& c : right_table->columns) {
            header.push_back(right_table->name + "." + c.name);
        }
    } else {
        header = stmt.columns;
    }

    if (on_header && !on_header(header)) {
        return false;
    }

    std::vector<std::pair<int, int>> proj_indexes;
    if (!all_cols) {
        proj_indexes.reserve(stmt.columns.size());
        for (const auto& c : stmt.columns) {
            const int li = find_column_index_fast(left_table.get(), c);
            if (li >= 0) {
                proj_indexes.push_back({0, li});
                continue;
            }
            const int ri = find_column_index_fast(right_table.get(), c);
            if (ri >= 0) {
                proj_indexes.push_back({1, ri});
                continue;
            }
            if (err) {
                *err = "unknown column in SELECT: " + c;
            }
            return false;
        }
    }

    int where_col_idx_left = -1;
    int where_col_idx_right = -1;
    if (stmt.has_where) {
        where_col_idx_left = find_column_index_fast(left_table.get(), stmt.where.column);
        where_col_idx_right = find_column_index_fast(right_table.get(), stmt.where.column);
    }

    auto emit_join_row = [&](const std::vector<std::string>& l_values,
                             const std::vector<std::string>& r_values) -> bool {
        if (stmt.has_where) {
            bool match = false;
            if (where_col_idx_left >= 0) {
                match = compare_by_type(
                    l_values[static_cast<std::size_t>(where_col_idx_left)],
                    stmt.where.value,
                    left_table->columns[static_cast<std::size_t>(where_col_idx_left)].type,
                    stmt.where.op);
            } else if (where_col_idx_right >= 0) {
                match = compare_by_type(
                    r_values[static_cast<std::size_t>(where_col_idx_right)],
                    stmt.where.value,
                    right_table->columns[static_cast<std::size_t>(where_col_idx_right)].type,
                    stmt.where.op);
            }
            if (!match) {
                return true;
            }
        }

        std::vector<std::string> out;
        if (all_cols) {
            out.reserve(l_values.size() + r_values.size());
            out.insert(out.end(), l_values.begin(), l_values.end());
            out.insert(out.end(), r_values.begin(), r_values.end());
        } else {
            out.reserve(proj_indexes.size());
            for (const auto& [table_id, idx] : proj_indexes) {
                if (table_id == 0) {
                    out.push_back(l_values[static_cast<std::size_t>(idx)]);
                } else {
                    out.push_back(r_values[static_cast<std::size_t>(idx)]);
                }
            }
        }

        if (on_row) {
            return on_row(out);
        }
        return true;
    };

    if (stmt.has_where && stmt.where.op == OpType::EQ) {
        if (left_table->has_primary_key && where_col_idx_left == static_cast<int>(left_table->pk_index)) {
            StoredRow left_row;
            std::string lookup_err;
            if (!storage_->get_row_by_pk(left_table->name, stmt.where.value, &left_row, &lookup_err)) {
                if (lookup_err == "row not found" || lookup_err == "row expired") {
                    return true;
                }
                if (err) {
                    *err = lookup_err;
                }
                return false;
            }

            if (right_table->has_primary_key && right_join_idx == static_cast<int>(right_table->pk_index)) {
                if (!storage_->with_row_by_pk(
                        right_table->name,
                        left_row.values[static_cast<std::size_t>(left_join_idx)],
                        [&](const std::vector<std::string>& r_values, std::int64_t) {
                            return emit_join_row(left_row.values, r_values);
                        },
                        &lookup_err)) {
                    if (err) {
                        *err = lookup_err;
                    }
                    return false;
                }
                return true;
            }

            if (!storage_->scan_table_fast(
                    right_table->name,
                    [&](std::size_t, const std::vector<std::string>& r_values, std::int64_t) {
                        if (r_values[static_cast<std::size_t>(right_join_idx)] !=
                            left_row.values[static_cast<std::size_t>(left_join_idx)]) {
                            return true;
                        }
                        return emit_join_row(left_row.values, r_values);
                    },
                    &lookup_err)) {
                if (err) {
                    *err = lookup_err;
                }
                return false;
            }
            return true;
        }

        if (right_table->has_primary_key && where_col_idx_right == static_cast<int>(right_table->pk_index)) {
            StoredRow right_row;
            std::string lookup_err;
            if (!storage_->get_row_by_pk(right_table->name, stmt.where.value, &right_row, &lookup_err)) {
                if (lookup_err == "row not found" || lookup_err == "row expired") {
                    return true;
                }
                if (err) {
                    *err = lookup_err;
                }
                return false;
            }

            if (left_table->has_primary_key && left_join_idx == static_cast<int>(left_table->pk_index)) {
                if (!storage_->with_row_by_pk(
                        left_table->name,
                        right_row.values[static_cast<std::size_t>(right_join_idx)],
                        [&](const std::vector<std::string>& l_values, std::int64_t) {
                            return emit_join_row(l_values, right_row.values);
                        },
                        &lookup_err)) {
                    if (err) {
                        *err = lookup_err;
                    }
                    return false;
                }
                return true;
            }

            if (!storage_->scan_table_fast(
                    left_table->name,
                    [&](std::size_t, const std::vector<std::string>& l_values, std::int64_t) {
                        if (l_values[static_cast<std::size_t>(left_join_idx)] !=
                            right_row.values[static_cast<std::size_t>(right_join_idx)]) {
                            return true;
                        }
                        return emit_join_row(l_values, right_row.values);
                    },
                    &lookup_err)) {
                if (err) {
                    *err = lookup_err;
                }
                return false;
            }
            return true;
        }
    }

    if (left_table->has_primary_key && left_join_idx == static_cast<int>(left_table->pk_index)) {
        if (!storage_->scan_table_fast(
                right_table->name,
                [&](std::size_t, const std::vector<std::string>& r_values, std::int64_t) {
                    std::string lookup_err;
                    if (!storage_->with_row_by_pk(
                            left_table->name,
                            r_values[static_cast<std::size_t>(right_join_idx)],
                            [&](const std::vector<std::string>& l_values, std::int64_t) {
                                return emit_join_row(l_values, r_values);
                            },
                            &lookup_err)) {
                        if (!lookup_err.empty()) {
                            local_err = lookup_err;
                        }
                        return false;
                    }
                    return true;
                },
                &local_err)) {
            if (err) {
                *err = local_err;
            }
            return false;
        }
        return true;
    }

    if (right_table->has_primary_key && right_join_idx == static_cast<int>(right_table->pk_index)) {
        if (!storage_->scan_table_fast(
                left_table->name,
                [&](std::size_t, const std::vector<std::string>& l_values, std::int64_t) {
                    std::string lookup_err;
                    if (!storage_->with_row_by_pk(
                            right_table->name,
                            l_values[static_cast<std::size_t>(left_join_idx)],
                            [&](const std::vector<std::string>& r_values, std::int64_t) {
                                return emit_join_row(l_values, r_values);
                            },
                            &lookup_err)) {
                        if (!lookup_err.empty()) {
                            local_err = lookup_err;
                        }
                        return false;
                    }
                    return true;
                },
                &local_err)) {
            if (err) {
                *err = local_err;
            }
            return false;
        }
        return true;
    }

    if (err) {
        *err = "JOIN requires one side of ON clause to be the primary key (index-based JOIN only)";
    }
    return false;
}

bool QueryExecutor::evaluate_condition(const Condition& cond,
                                       const std::vector<ColumnDef>& cols,
                                       const std::vector<std::string>& row,
                                       const std::string& table_name) const {
    const int idx = find_column_index(cols, table_name, cond.column);
    if (idx < 0) {
        return false;
    }
    const auto uidx = static_cast<std::size_t>(idx);
    if (uidx >= row.size() || uidx >= cols.size()) {
        return false;
    }
    return compare_by_type(row[uidx], cond.value, cols[uidx].type, cond.op);
}

bool QueryExecutor::compare_by_type(const std::string& lhs,
                                    const std::string& rhs,
                                    DataType type,
                                    OpType op) const {
    auto cmp_long = [&](long long a, long long b) {
        switch (op) {
            case OpType::EQ:
                return a == b;
            case OpType::NE:
                return a != b;
            case OpType::LT:
                return a < b;
            case OpType::LE:
                return a <= b;
            case OpType::GT:
                return a > b;
            case OpType::GE:
                return a >= b;
        }
        return false;
    };

    auto cmp_double = [&](double a, double b) {
        switch (op) {
            case OpType::EQ:
                return a == b;
            case OpType::NE:
                return a != b;
            case OpType::LT:
                return a < b;
            case OpType::LE:
                return a <= b;
            case OpType::GT:
                return a > b;
            case OpType::GE:
                return a >= b;
        }
        return false;
    };

    auto cmp_string = [&](const std::string& a, const std::string& b) {
        switch (op) {
            case OpType::EQ:
                return a == b;
            case OpType::NE:
                return a != b;
            case OpType::LT:
                return a < b;
            case OpType::LE:
                return a <= b;
            case OpType::GT:
                return a > b;
            case OpType::GE:
                return a >= b;
        }
        return false;
    };

    try {
        if (type == DataType::INT) {
            return cmp_long(std::stoll(lhs), std::stoll(rhs));
        }
        if (type == DataType::DECIMAL) {
            return cmp_double(std::stod(lhs), std::stod(rhs));
        }
        if (type == DataType::DATETIME) {
            std::int64_t a = 0;
            std::int64_t b = 0;
            if (!utils::parse_datetime_to_epoch(lhs, &a) || !utils::parse_datetime_to_epoch(rhs, &b)) {
                return false;
            }
            return cmp_long(a, b);
        }
        return cmp_string(lhs, rhs);
    } catch (...) {
        return false;
    }
}

int QueryExecutor::find_column_index(const std::vector<ColumnDef>& cols,
                                     const std::string& table_name,
                                     const std::string& name) const {
    const auto dot = name.find('.');
    std::string raw = name;
    if (dot != std::string::npos) {
        const auto prefix = name.substr(0, dot);
        if (!table_name.empty() && prefix != table_name) {
            return -1;
        }
        raw = name.substr(dot + 1);
    }

    for (std::size_t i = 0; i < cols.size(); ++i) {
        if (cols[i].name == raw) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

int QueryExecutor::find_column_index_fast(const Table* table, const std::string& name) const {
    if (!table) {
        return -1;
    }

    // Handle table.column syntax
    const auto dot = name.find('.');
    std::string raw = name;
    if (dot != std::string::npos) {
        const auto prefix = name.substr(0, dot);
        if (prefix != table->name) {
            return -1;  // Wrong table prefix
        }
        raw = name.substr(dot + 1);
    }

    // Use cached column index map for O(1) lookup
    return table->find_column(raw);
}

bool QueryExecutor::try_fast_insert_path(std::string_view sql, ExecResult* out) {
    if (!out) {
        return false;
    }
    if (!ci_starts_with_insert(sql.data(), sql.size())) {
        return false;
    }

    Arena& arena = get_thread_arena();
    if (sql.size() > 0) {
        constexpr std::size_t kArenaGrowthFactor = 8;
        const std::size_t desired = std::max<std::size_t>(
            kArenaSize,
            sql.size() * kArenaGrowthFactor + 4096);
        if (arena.capacity < desired) {
            arena.ensure_capacity(desired);
        }
    }
    arena.reset();

    FastInsertStmt stmt{};
    std::string parse_err;
    if (!parser_.parse_insert_fast(sql.data(), sql.size(), &arena, &stmt, &parse_err)) {
        // Fall back to the standard regex parser path
        return false;
    }

    *out = execute_insert_fast(stmt);
    return true;
}

ExecResult QueryExecutor::execute_insert_fast(const FastInsertStmt& stmt) {
    const std::string table_name(stmt.table_name.data, stmt.table_name.length);

    struct InsertTableCache {
        std::string table_name;
        std::weak_ptr<Table> table;
    };
    static thread_local InsertTableCache cache;

    std::shared_ptr<Table> table;
    std::string err;
    if (cache.table_name == table_name) {
        table = cache.table.lock();
    }
    if (!table) {
        if (!storage_->get_table(table_name, &table, &err)) {
            return ExecResult{false, err, {}};
        }
        cache.table_name = table_name;
        cache.table = table;
    }

    const std::uint32_t row_count = stmt.row_count > 0 ? stmt.row_count : 1;
    if (stmt.value_count == 0 || row_count == 0) {
        return ExecResult{false, "INSERT has no VALUES", {}};
    }

    std::vector<int> column_map;
    const bool has_explicit_columns = (stmt.columns && stmt.column_count > 0);
    const std::size_t ncols = table->columns.size();
    if (has_explicit_columns) {
        if (stmt.column_count != stmt.value_count) {
            return ExecResult{false, "column count does not match values count", {}};
        }
        column_map.resize(stmt.column_count, -1);
        for (std::uint32_t i = 0; i < stmt.column_count; ++i) {
            const std::string col_name(stmt.columns[i].data, stmt.columns[i].length);
            const int col_idx = table->find_column(col_name);
            if (col_idx < 0) {
                return ExecResult{false, "unknown column in INSERT: " + col_name, {}};
            }
            column_map[i] = col_idx;
        }
    }

    std::int64_t expires = 0;
    if (stmt.has_expires_at) {
        expires = stmt.expires_at;
    } else if (stmt.has_ttl) {
        expires = utils::now_epoch_seconds() + stmt.ttl_seconds;
    } else {
        expires = utils::now_epoch_seconds() + 24 * 60 * 60;
    }

    std::vector<ArenaStringView> ordered_values;
    if (!has_explicit_columns) {
        if (!storage_->insert_rows_fast(table_name,
                                        stmt.values,
                                        stmt.value_count,
                                        row_count,
                                        expires,
                                        &err)) {
            return ExecResult{false, err, {}};
        }
        return ExecResult{true, "OK", {}};
    }

    ordered_values.resize(ncols);

    for (std::uint32_t row = 0; row < row_count; ++row) {
        const ArenaStringView* row_values = stmt.values + static_cast<std::size_t>(row) * stmt.value_count;

        for (std::size_t i = 0; i < ncols; ++i) {
            ordered_values[i].data = nullptr;
            ordered_values[i].length = 0;
        }

        for (std::uint32_t i = 0; i < stmt.column_count; ++i) {
            ordered_values[static_cast<std::size_t>(column_map[i])] = row_values[i];
        }

        for (std::size_t i = 0; i < ncols; ++i) {
            if (!ordered_values[i].data) {
                return ExecResult{false, "missing value for column: " + table->columns[i].name, {}};
            }
        }

        if (!storage_->insert_row_fast(table_name,
                                       ordered_values.data(),
                                       static_cast<std::uint32_t>(ncols),
                                       expires,
                                       &err)) {
            return ExecResult{false, err, {}};
        }
    }
    return ExecResult{true, "OK", {}};
}

}  // namespace flexql
