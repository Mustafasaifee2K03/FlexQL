#include <chrono>
#include <algorithm>
#include <cmath>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "common/flexql.h"

namespace {

constexpr int64_t kUsersToInsert = 5'000'000;
constexpr int64_t kOrdersToInsert = 5'000'000;
constexpr int kUsersColsDefault = 4;
constexpr int kOrdersColsDefault = 4;
constexpr int kProgressStep = 100'000;
constexpr int kBulkInsertBatchDefault = 10'000;
constexpr int64_t kLongTtlSeconds = 10LL * 365 * 24 * 60 * 60;

enum class DataType {
    Int,
    Decimal,
    Varchar,
    Datetime,
};

struct ColumnSpec {
    std::string name;
    DataType type;
};

struct TableSchema {
    std::string table_name;
    std::vector<ColumnSpec> columns;
};

struct CallbackState {
    int printed = 0;
    int max_print = 5;
    int total_seen = 0;
    bool verbose = false;
};

std::string random_datetime(std::mt19937_64 &rng);

std::mt19937_64 &global_rng() {
    static std::mt19937_64 rng(42);
    return rng;
}

std::string format_with_commas(int64_t value) {
    std::string s = std::to_string(value);
    int insert_pos = static_cast<int>(s.size()) - 3;
    while (insert_pos > 0) {
        s.insert(static_cast<std::size_t>(insert_pos), ",");
        insert_pos -= 3;
    }
    return s;
}

std::string format_decimal(double value) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.2f", value);
    return std::string(buf);
}

std::string type_to_sql(DataType type) {
    switch (type) {
        case DataType::Int:
            return "INT";
        case DataType::Decimal:
            return "DECIMAL";
        case DataType::Varchar:
            return "VARCHAR";
        case DataType::Datetime:
            return "DATETIME";
    }
    return "INT";
}

std::string random_type() {
    static const std::array<std::string, 4> kTypes = {"INT", "DECIMAL", "VARCHAR", "DATETIME"};
    std::uniform_int_distribution<int> type_dist(0, static_cast<int>(kTypes.size()) - 1);
    return kTypes[static_cast<std::size_t>(type_dist(global_rng()))];
}

DataType parse_data_type(const std::string &type) {
    if (type == "INT") {
        return DataType::Int;
    }
    if (type == "DECIMAL") {
        return DataType::Decimal;
    }
    if (type == "VARCHAR") {
        return DataType::Varchar;
    }
    return DataType::Datetime;
}

TableSchema build_users_schema(int users_cols) {
    TableSchema schema;
    schema.table_name = "USERS";
    schema.columns.push_back({"ID", DataType::Int});

    const int generated_cols = std::max(0, users_cols - 1);
    for (int i = 1; i <= generated_cols; ++i) {
        schema.columns.push_back({"COL" + std::to_string(i), parse_data_type(random_type())});
    }
    return schema;
}

TableSchema build_orders_schema(int orders_cols) {
    TableSchema schema;
    schema.table_name = "ORDERS";
    schema.columns.push_back({"ORDER_ID", DataType::Int});
    schema.columns.push_back({"USER_ID", DataType::Int});

    const int generated_cols = std::max(0, orders_cols - 2);
    for (int i = 1; i <= generated_cols; ++i) {
        schema.columns.push_back({"COL" + std::to_string(i), parse_data_type(random_type())});
    }
    return schema;
}

std::string create_table_sql(const TableSchema &schema) {
    std::ostringstream sql;
    sql << "CREATE TABLE " << schema.table_name << " (";
    for (std::size_t i = 0; i < schema.columns.size(); ++i) {
        const auto &column = schema.columns[i];
        if (i > 0) {
            sql << ", ";
        }
        sql << column.name << " " << type_to_sql(column.type);
        if (schema.table_name == "USERS" && column.name == "ID") {
            sql << " PRIMARY KEY";
        }
        if (schema.table_name == "ORDERS" && column.name == "ORDER_ID") {
            sql << " PRIMARY KEY";
        }
    }
    sql << ");";
    return sql.str();
}

std::string random_value_for_type(DataType type,
                                  int64_t row_id,
                                  const std::string &column_name,
                                  std::mt19937_64 &rng) {
    std::uniform_int_distribution<int> int_dist(1, 1'000'000);
    std::uniform_real_distribution<double> decimal_dist(1.0, 100000.0);

    switch (type) {
        case DataType::Int:
            return std::to_string(int_dist(rng));
        case DataType::Decimal:
            return format_decimal(decimal_dist(rng));
        case DataType::Varchar:
            return "'text_" + std::to_string(row_id) + "_" + column_name + "'";
        case DataType::Datetime:
            return "'" + random_datetime(rng) + "'";
    }
    return "0";
}

std::string build_insert_sql(const TableSchema &schema,
                             int64_t primary_id,
                             int64_t user_id,
                             std::mt19937_64 &rng) {
    std::ostringstream sql;
    sql << "INSERT INTO " << schema.table_name << " VALUES (";

    for (std::size_t i = 0; i < schema.columns.size(); ++i) {
        const auto &column = schema.columns[i];
        if (i > 0) {
            sql << ", ";
        }

        if (schema.table_name == "USERS" && column.name == "ID") {
            sql << primary_id;
            continue;
        }

        if (schema.table_name == "ORDERS" && column.name == "ORDER_ID") {
            sql << primary_id;
            continue;
        }

        if (schema.table_name == "ORDERS" && column.name == "USER_ID") {
            sql << user_id;
            continue;
        }

        sql << random_value_for_type(column.type, primary_id, column.name, rng);
    }

    sql << ") TTL " << kLongTtlSeconds << ";";
    return sql.str();
}

std::string users_where_column(const TableSchema &users_schema) {
    for (const auto &column : users_schema.columns) {
        if (column.type == DataType::Int && column.name != "ID") {
            return column.name;
        }
    }
    return "ID";
}

std::string random_datetime(std::mt19937_64 &rng) {
    const std::time_t now = std::time(nullptr);
    const std::time_t start = now - (365 * 24 * 60 * 60);
    std::uniform_int_distribution<int64_t> offset_dist(0, 365LL * 24 * 60 * 60);
    const std::time_t ts = start + static_cast<std::time_t>(offset_dist(rng));

    std::tm tm_val {};
#if defined(_WIN32)
    gmtime_s(&tm_val, &ts);
#else
    gmtime_r(&ts, &tm_val);
#endif

    char out[32];
    std::strftime(out, sizeof(out), "%Y-%m-%d %H:%M:%S", &tm_val);
    return std::string(out);
}

int benchmark_callback(void *data, int column_count, char **values, char **column_names) {
    auto *state = static_cast<CallbackState *>(data);
    if (!state) {
        return 0;
    }

    ++state->total_seen;
    if (state->verbose && state->printed < state->max_print) {
        for (int i = 0; i < column_count; ++i) {
            std::cout << column_names[i] << "=" << values[i];
            if (i + 1 < column_count) {
                std::cout << " | ";
            }
        }
        std::cout << "\n";
        ++state->printed;
    }

    if (state->verbose && state->printed >= state->max_print) {
        return 1;
    }
    return 0;
}

bool exec_sql(FlexQL *db,
              const std::string &sql,
              int (*callback)(void *, int, char **, char **),
              void *arg,
              bool ignore_error = false) {
    char *errmsg = nullptr;
    const int rc = flexql_exec(db, sql.c_str(), callback, arg, &errmsg);
    if (rc == FLEXQL_OK) {
        return true;
    }

    std::string err = errmsg ? errmsg : "unknown error";
    if (errmsg) {
        flexql_free(errmsg);
    }

    if (ignore_error) {
        std::cerr << "[warn] " << err << "\n";
        return true;
    }

    std::cerr << "[error] SQL failed: " << err << "\n";
    std::cerr << "[error] Statement: " << sql << "\n";
    return false;
}

bool exec_sql_bulk(FlexQL *db, const std::vector<std::string> &sql_batch) {
    if (!db || sql_batch.empty()) {
        return true;
    }

    std::vector<const char*> sql_ptrs;
    sql_ptrs.reserve(sql_batch.size());
    for (const auto &sql : sql_batch) {
        sql_ptrs.push_back(sql.c_str());
    }

    char *errmsg = nullptr;
    const int rc = flexql_exec_bulk(
        db,
        sql_ptrs.data(),
        static_cast<int>(sql_ptrs.size()),
        &errmsg);
    if (rc == FLEXQL_OK) {
        return true;
    }

    std::string err = errmsg ? errmsg : "unknown error";
    if (errmsg) {
        flexql_free(errmsg);
    }
    std::cerr << "[error] BULK SQL failed: " << err << "\n";
    return false;
}

bool run_insert_benchmark(FlexQL *db,
                          int64_t users_target,
                          int64_t orders_target,
                          const TableSchema &users_schema,
                          const TableSchema &orders_schema) {
    std::mt19937_64 rng(42);
    int bulk_batch = kBulkInsertBatchDefault;
    if (const char* env_batch = std::getenv("FLEXQL_BULK_BATCH"); env_batch && *env_batch != '\0') {
        const int parsed = std::atoi(env_batch);
        if (parsed > 0) {
            bulk_batch = parsed;
        }
    }

    std::vector<std::string> batch;
    batch.reserve(static_cast<std::size_t>(bulk_batch));

    std::cout << "\n[phase] Inserting USERS rows...\n";
    const auto users_start = std::chrono::high_resolution_clock::now();
    for (int64_t i = 1; i <= users_target; ++i) {
        batch.push_back(build_insert_sql(users_schema, i, 0, rng));

        if (batch.size() >= static_cast<std::size_t>(bulk_batch) || i == users_target) {
            if (!exec_sql_bulk(db, batch)) {
                return false;
            }
            batch.clear();
        }

        if (i % kProgressStep == 0) {
            const auto now = std::chrono::high_resolution_clock::now();
            const double elapsed = std::chrono::duration<double>(now - users_start).count();
            std::cout << "  USERS progress: " << format_with_commas(i) << "/" << format_with_commas(users_target)
                      << " inserted (" << std::fixed << std::setprecision(2) << elapsed << " sec)\n";
        }
    }
    const auto users_end = std::chrono::high_resolution_clock::now();
    const double users_elapsed = std::chrono::duration<double>(users_end - users_start).count();

    std::cout << "\n[phase] Inserting ORDERS rows...\n";
    const auto orders_start = std::chrono::high_resolution_clock::now();
    std::uniform_int_distribution<int64_t> user_id_dist(1, users_target);
    for (int64_t i = 1; i <= orders_target; ++i) {
        const int64_t user_id = user_id_dist(rng);
        batch.push_back(build_insert_sql(orders_schema, i, user_id, rng));

        if (batch.size() >= static_cast<std::size_t>(bulk_batch) || i == orders_target) {
            if (!exec_sql_bulk(db, batch)) {
                return false;
            }
            batch.clear();
        }

        if (i % kProgressStep == 0) {
            const auto now = std::chrono::high_resolution_clock::now();
            const double elapsed = std::chrono::duration<double>(now - orders_start).count();
            std::cout << "  ORDERS progress: " << format_with_commas(i) << "/" << format_with_commas(orders_target)
                      << " inserted (" << std::fixed << std::setprecision(2) << elapsed << " sec)\n";
        }
    }
    const auto orders_end = std::chrono::high_resolution_clock::now();
    const double orders_elapsed = std::chrono::duration<double>(orders_end - orders_start).count();

    std::cout << "\nInserted " << format_with_commas(users_target) << " USERS in "
              << std::fixed << std::setprecision(2) << users_elapsed << " sec\n";
    std::cout << "Inserted " << format_with_commas(orders_target) << " ORDERS in "
              << std::fixed << std::setprecision(2) << orders_elapsed << " sec\n";
    std::cout << "Insert time total: " << std::fixed << std::setprecision(2)
              << (users_elapsed + orders_elapsed) << " sec\n";

    return true;
}

bool run_timed_query(FlexQL *db,
                     const std::string &title,
                     const std::string &sql) {
    std::cout << "\nQuery: " << title << "\n\n";

    const auto start = std::chrono::high_resolution_clock::now();
    const bool benchmark_ok = exec_sql(db, sql, nullptr, nullptr);
    const auto end = std::chrono::high_resolution_clock::now();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    if (!benchmark_ok) {
        return false;
    }

    std::cout << "Time: " << ms << " ms\n\n";
    std::cout << "Sample rows (first 5):\n";

    CallbackState state;
    const char* verbose_env = std::getenv("FLEXQL_BENCH_VERBOSE");
    state.verbose = true;
    if (verbose_env && std::strcmp(verbose_env, "0") == 0) {
        state.verbose = false;
    }
    state.max_print = 5;

    const bool sample_ok = exec_sql(db, sql, benchmark_callback, &state);
    if (!sample_ok) {
        return false;
    }

    if (state.total_seen == 0) {
        std::cout << "(no rows)\n";
    } else if (!state.verbose) {
        std::cout << "(" << state.total_seen << " rows matched; set FLEXQL_BENCH_VERBOSE=1 to print samples)\n";
    }

    return true;
}

}  // namespace

int main(int argc, char **argv) {
    int64_t users_target = kUsersToInsert;
    int64_t orders_target = kOrdersToInsert;
    int users_cols = kUsersColsDefault;
    int orders_cols = kOrdersColsDefault;

    if (argc >= 5) {
        users_target = std::atoll(argv[1]);
        orders_target = std::atoll(argv[2]);
        users_cols = std::atoi(argv[3]);
        orders_cols = std::atoi(argv[4]);
    } else {
        if (argc >= 2) {
            users_target = std::strtoll(argv[1], nullptr, 10);
        }
        if (argc >= 3) {
            orders_target = std::strtoll(argv[2], nullptr, 10);
        }
    }

    users_cols = std::max(users_cols, 1);
    orders_cols = std::max(orders_cols, 2);

    TableSchema users_schema = build_users_schema(users_cols);
    TableSchema orders_schema = build_orders_schema(orders_cols);

    std::cout << "----- FLEXQL BENCHMARK -----\n";
    std::cout << "\n";
    std::cout << "USERS rows: " << format_with_commas(users_target) << "\n";
    std::cout << "ORDERS rows: " << format_with_commas(orders_target) << "\n";
    std::cout << "USERS columns: " << users_cols << "\n";
    std::cout << "ORDERS columns: " << orders_cols << "\n";

    FlexQL *db = nullptr;
    if (flexql_open("127.0.0.1", 9000, &db) != FLEXQL_OK || db == nullptr) {
        std::cerr << "Failed to connect to FlexQL server at 127.0.0.1:9000\n";
        return 1;
    }

    // If tables already exist, the benchmark continues with warnings.
    if (!exec_sql(db, create_table_sql(users_schema), nullptr, nullptr, true)) {
        (void)flexql_close(db);
        return 1;
    }

    if (!exec_sql(db, create_table_sql(orders_schema), nullptr, nullptr, true)) {
        (void)flexql_close(db);
        return 1;
    }

    const auto insert_start = std::chrono::high_resolution_clock::now();
    if (!run_insert_benchmark(db, users_target, orders_target, users_schema, orders_schema)) {
        (void)flexql_close(db);
        return 1;
    }
    const auto insert_end = std::chrono::high_resolution_clock::now();
    const double insert_total = std::chrono::duration<double>(insert_end - insert_start).count();

    std::cout << "\nUsers inserted: " << format_with_commas(users_target) << "\n";
    std::cout << "Orders inserted: " << format_with_commas(orders_target) << "\n";
    std::cout << "Insert time: " << std::fixed << std::setprecision(2) << insert_total << " sec\n";

    if (!run_timed_query(db, "SELECT * FROM USERS", "SELECT * FROM USERS;")) {
        (void)flexql_close(db);
        return 1;
    }

    if (!run_timed_query(db,
                         "SELECT * FROM USERS WHERE ID = 1000000",
                         "SELECT * FROM USERS WHERE ID = 1000000;")) {
        (void)flexql_close(db);
        return 1;
    }

    const std::string users_filter_query = "SELECT * FROM USERS WHERE ID = 500;";

    if (!run_timed_query(db,
                         "SELECT * FROM USERS WHERE ID = 500",
                         users_filter_query)) {
        (void)flexql_close(db);
        return 1;
    }

    int64_t non_key_probe = 500;
    if (const char* env_non_key_probe = std::getenv("FLEXQL_NON_KEY_PROBE");
        env_non_key_probe && *env_non_key_probe != '\0') {
        non_key_probe = std::strtoll(env_non_key_probe, nullptr, 10);
    }

    const std::string non_key_query =
        "SELECT * FROM ORDERS WHERE USER_ID = " + std::to_string(non_key_probe) + ";";
    if (!run_timed_query(db,
                         "SELECT * FROM ORDERS WHERE USER_ID = " + std::to_string(non_key_probe),
                         non_key_query)) {
        (void)flexql_close(db);
        return 1;
    }

    // if (!run_timed_query(db,
    //                      "SELECT * FROM USERS INNER JOIN ORDERS ON USERS.ID = ORDERS.USER_ID",
    //                      "SELECT * FROM USERS INNER JOIN ORDERS ON USERS.ID = ORDERS.USER_ID;")) {
    //     (void)flexql_close(db);
    //     return 1;
    // }

    if (!run_timed_query(db,
                         "SELECT * FROM USERS INNER JOIN ORDERS ON USERS.ID = ORDERS.USER_ID WHERE ORDERS.ORDER_ID = 1",
                         "SELECT * FROM USERS INNER JOIN ORDERS ON USERS.ID = ORDERS.USER_ID WHERE ORDERS.ORDER_ID = 1;")) {
        (void)flexql_close(db);
        return 1;
    }

    if (flexql_close(db) != FLEXQL_OK) {
        std::cerr << "Failed to close FlexQL handle\n";
        return 1;
    }

    std::cout << "\n----- BENCHMARK COMPLETE -----\n";
    return 0;
}
