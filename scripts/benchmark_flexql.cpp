#include <iostream>
#include <chrono>
#include <charconv>
#include <atomic>
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <limits>
#include <string>
#include <thread>
#include <vector>
#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_INSERT_ROWS = 10000000LL;
static const int DEFAULT_TOTAL_COLUMNS = 5;
static const int DEFAULT_BATCH_SIZE = 5000;
static const int MAX_BATCH_SIZE = 100000;
static const int DEFAULT_CLIENT_THREADS = 8;
static const int MAX_CLIENT_THREADS = 256;
static const size_t MAX_BULK_REQUEST_BYTES = 32ULL * 1024ULL * 1024ULL;

static inline char* append_u64(char *out, unsigned long long value) {
    char tmp[32];
    int n = 0;
    do {
        tmp[n++] = static_cast<char>('0' + (value % 10ULL));
        value /= 10ULL;
    } while (value != 0ULL);
    while (n > 0) {
        *out++ = tmp[--n];
    }
    return out;
}

static inline void append_i64(string &out, long long value) {
    char buf[32];
    auto conv = std::to_chars(buf, buf + sizeof(buf), value);
    if (conv.ec == std::errc()) {
        out.append(buf, static_cast<size_t>(conv.ptr - buf));
        return;
    }
    out += to_string(value);
}

struct BenchOptions {
    string host = "127.0.0.1";
    int port = 9000;
    long long rows = DEFAULT_INSERT_ROWS;
    int columns = DEFAULT_TOTAL_COLUMNS;
    int batch_size = DEFAULT_BATCH_SIZE;
    int client_threads = DEFAULT_CLIENT_THREADS;
    bool run_unit_tests_only = false;
    bool skip_unit_tests = false;
};

static void print_usage(const char *prog) {
    cout << "Usage:\n";
    cout << "  " << prog << " [rows] [columns>=5] [batch_size<=" << MAX_BATCH_SIZE << "]\n";
    cout << "  " << prog << " [options]\n\n";
    cout << "Options:\n";
    cout << "  --rows <n>            Number of rows to insert\n";
    cout << "  --cols <n>            Total columns (>=5)\n";
    cout << "  --batch <n>           Bulk batch size (1.." << MAX_BATCH_SIZE << ")\n";
    cout << "  --host <addr>         Server host (default 127.0.0.1)\n";
    cout << "  --port <n>            Server port (default 9000)\n";
    cout << "  --clients <n>         Parallel client threads (1.." << MAX_CLIENT_THREADS << ")\n";
    cout << "  --unit-test           Run only unit tests\n";
    cout << "  --skip-unit-tests     Skip unit tests after benchmark\n";
    cout << "  -h, --help            Show this message\n";
}

static bool parse_int_arg(const string &value, int *out) {
    if (!out || value.empty()) {
        return false;
    }
    char *end = nullptr;
    long v = std::strtol(value.c_str(), &end, 10);
    if (!end || *end != '\0') {
        return false;
    }
    if (v < std::numeric_limits<int>::min() || v > std::numeric_limits<int>::max()) {
        return false;
    }
    *out = static_cast<int>(v);
    return true;
}

static bool parse_i64_arg(const string &value, long long *out) {
    if (!out || value.empty()) {
        return false;
    }
    char *end = nullptr;
    long long v = std::strtoll(value.c_str(), &end, 10);
    if (!end || *end != '\0') {
        return false;
    }
    *out = v;
    return true;
}

static bool parse_benchmark_args(int argc, char **argv, BenchOptions *opts) {
    if (!opts) {
        return false;
    }

    vector<string> positional;
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "--unit-test") {
            opts->run_unit_tests_only = true;
            continue;
        }
        if (arg == "--skip-unit-tests") {
            opts->skip_unit_tests = true;
            continue;
        }
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return false;
        }

        auto parse_kv = [&](const char *flag, string *value_out) -> bool {
            const size_t n = std::strlen(flag);
            if (arg.compare(0, n, flag) != 0) {
                return false;
            }
            if (arg.size() > n && arg[n] == '=') {
                *value_out = arg.substr(n + 1);
                return true;
            }
            if (arg.size() == n && i + 1 < argc) {
                *value_out = argv[++i];
                return true;
            }
            return false;
        };

        string value;
        if (parse_kv("--rows", &value)) {
            if (!parse_i64_arg(value, &opts->rows)) {
                cout << "Invalid --rows value: " << value << "\n";
                return false;
            }
            continue;
        }
        if (parse_kv("--cols", &value)) {
            if (!parse_int_arg(value, &opts->columns)) {
                cout << "Invalid --cols value: " << value << "\n";
                return false;
            }
            continue;
        }
        if (parse_kv("--batch", &value)) {
            if (!parse_int_arg(value, &opts->batch_size)) {
                cout << "Invalid --batch value: " << value << "\n";
                return false;
            }
            continue;
        }
        if (parse_kv("--port", &value)) {
            if (!parse_int_arg(value, &opts->port)) {
                cout << "Invalid --port value: " << value << "\n";
                return false;
            }
            continue;
        }
        if (parse_kv("--clients", &value)) {
            if (!parse_int_arg(value, &opts->client_threads)) {
                cout << "Invalid --clients value: " << value << "\n";
                return false;
            }
            continue;
        }
        if (parse_kv("--host", &value)) {
            opts->host = value;
            continue;
        }

        if (!arg.empty() && arg[0] == '-') {
            cout << "Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return false;
        }

        positional.push_back(arg);
    }

    if (!opts->run_unit_tests_only) {
        if (!positional.empty() && !parse_i64_arg(positional[0], &opts->rows)) {
            cout << "Invalid row count: " << positional[0] << "\n";
            return false;
        }
        if (positional.size() > 1 && !parse_int_arg(positional[1], &opts->columns)) {
            cout << "Invalid columns value: " << positional[1] << "\n";
            return false;
        }
        if (positional.size() > 2 && !parse_int_arg(positional[2], &opts->batch_size)) {
            cout << "Invalid batch size value: " << positional[2] << "\n";
            return false;
        }
        if (positional.size() > 3) {
            cout << "Too many positional arguments.\n";
            print_usage(argv[0]);
            return false;
        }
    }

    if (opts->rows <= 0) {
        cout << "Invalid row count. Use a positive integer.\n";
        return false;
    }
    if (opts->columns < 5) {
        cout << "Invalid total columns. Use an integer >= 5.\n";
        return false;
    }
    if (opts->batch_size <= 0 || opts->batch_size > MAX_BATCH_SIZE) {
        cout << "Invalid batch size. Use an integer in 1.." << MAX_BATCH_SIZE << ".\n";
        return false;
    }
    if (opts->port <= 0 || opts->port > 65535) {
        cout << "Invalid port. Use an integer in 1..65535.\n";
        return false;
    }
    if (opts->client_threads <= 0 || opts->client_threads > MAX_CLIENT_THREADS) {
        cout << "Invalid client threads. Use an integer in 1.." << MAX_CLIENT_THREADS << ".\n";
        return false;
    }

    return true;
}

struct QueryStats {
    long long rows = 0;
};

static int count_rows_callback(void *data, int argc, char **argv, char **azColName) {
    (void)argc;
    (void)argv;
    (void)azColName;
    QueryStats *stats = static_cast<QueryStats*>(data);
    if (stats) {
        stats->rows++;
    }
    return 0;
}

static bool run_exec(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool run_exec_allow_table_exists(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        string msg = errMsg ? errMsg : "unknown error";
        if (errMsg) {
            flexql_free(errMsg);
        }

        if (msg.find("table already exists") != string::npos) {
            cout << "[PASS] " << label << " (already exists) (" << elapsed << " ms)\n";
            return true;
        }

        cout << "[FAIL] " << label << " -> " << msg << "\n";
        return false;
    }

    cout << "[PASS] " << label << " (" << elapsed << " ms)\n";
    return true;
}

static bool run_query(FlexQL *db, const string &sql, const string &label) {
    QueryStats stats;
    char *errMsg = nullptr;
    auto start = high_resolution_clock::now();
    int rc = flexql_exec(db, sql.c_str(), count_rows_callback, &stats, &errMsg);
    auto end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(end - start).count();

    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << label << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "[PASS] " << label << " | rows=" << stats.rows << " | " << elapsed << " ms\n";
    return true;
}

static bool query_rows(FlexQL *db, const string &sql, vector<string> &out_rows) {
    struct Collector {
        vector<string> rows;
    } collector;

    auto cb = [](void *data, int argc, char **argv, char **azColName) -> int {
        (void)azColName;
        Collector *c = static_cast<Collector*>(data);
        string row;
        for (int i = 0; i < argc; ++i) {
            if (i > 0) {
                row += "|";
            }
            row += (argv[i] ? argv[i] : "NULL");
        }
        c->rows.push_back(row);
        return 0;
    };

    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), cb, &collector, &errMsg);
    if (rc != FLEXQL_OK) {
        cout << "[FAIL] " << sql << " -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    out_rows = collector.rows;
    return true;
}

static bool assert_rows_equal(const string &label, const vector<string> &actual, const vector<string> &expected) {
    if (actual == expected) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << "\n";
    cout << "Expected (" << expected.size() << "):\n";
    for (const auto &r : expected) {
        cout << "  " << r << "\n";
    }
    cout << "Actual (" << actual.size() << "):\n";
    for (const auto &r : actual) {
        cout << "  " << r << "\n";
    }
    return false;
}

static bool expect_query_failure(FlexQL *db, const string &sql, const string &label) {
    char *errMsg = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errMsg);
    if (rc == FLEXQL_OK) {
        cout << "[FAIL] " << label << " (expected failure, got success)\n";
        return false;
    }
    if (errMsg) {
        flexql_free(errMsg);
    }
    cout << "[PASS] " << label << "\n";
    return true;
}

static bool assert_row_count(const string &label, const vector<string> &rows, size_t expected_count) {
    if (rows.size() == expected_count) {
        cout << "[PASS] " << label << "\n";
        return true;
    }

    cout << "[FAIL] " << label << " (expected " << expected_count << ", got " << rows.size() << ")\n";
    return false;
}

static bool run_data_level_unit_tests(FlexQL *db) {
    cout << "\n\n[[ Running Unit Tests ]]\n\n";

    bool all_ok = true;
    int total_tests = 0;
    int failed_tests = 0;

    auto record = [&](bool result) {
        total_tests++;
        if (!result) {
            all_ok = false;
            failed_tests++;
        }
    };

    record(run_exec_allow_table_exists(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_USERS(ID DECIMAL PRIMARY KEY, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_USERS"));

    record(run_exec(db, "DELETE FROM TEST_USERS;", "RESET TEST_USERS"));

    record(run_exec(
            db,
            "INSERT INTO TEST_USERS VALUES "
            "(1, 'Alice', 1200, 1893456000),"
            "(2, 'Bob', 450, 1893456000),"
            "(3, 'Carol', 2200, 1893456000),"
            "(4, 'Dave', 800, 1893456000);",
            "INSERT TEST_USERS"));

    vector<string> rows;

    bool q1 = query_rows(db, "SELECT NAME, BALANCE FROM TEST_USERS WHERE ID = 2;", rows);
    record(q1);
    if (q1) {
        record(assert_rows_equal("Single-row value validation", rows, {"Bob 450"}));
    }

    bool q2 = query_rows(db, "SELECT NAME FROM TEST_USERS WHERE BALANCE > 1000 ORDER BY NAME;", rows);
    record(q2);
    if (q2) {
        record(assert_rows_equal("Filtered rows validation", rows, {"Alice", "Carol"}));
    }

    bool q3 = query_rows(db, "SELECT NAME FROM TEST_USERS ORDER BY BALANCE DESC;", rows);
    record(q3);
    if (q3) {
        record(assert_rows_equal("ORDER BY descending validation", rows, {"Carol", "Alice", "Dave", "Bob"}));
    }

    bool q4 = query_rows(db, "SELECT ID FROM TEST_USERS WHERE BALANCE > 5000;", rows);
    record(q4);
    if (q4) {
        record(assert_row_count("Empty result-set validation", rows, 0));
    }

    record(run_exec_allow_table_exists(
            db,
            "CREATE TABLE IF NOT EXISTS TEST_ORDERS(ORDER_ID DECIMAL PRIMARY KEY, USER_ID DECIMAL, AMOUNT DECIMAL, EXPIRES_AT DECIMAL);",
            "CREATE TABLE TEST_ORDERS"));

    record(run_exec(db, "DELETE FROM TEST_ORDERS;", "RESET TEST_ORDERS"));

    record(run_exec(
            db,
            "INSERT INTO TEST_ORDERS VALUES "
            "(101, 1, 50, 1893456000),"
            "(102, 1, 150, 1893456000),"
            "(103, 3, 500, 1893456000);",
            "INSERT TEST_ORDERS"));

    bool q5 = query_rows(
            db,
            "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
            "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
            "WHERE TEST_ORDERS.AMOUNT >= 100 ORDER BY TEST_ORDERS.ORDER_ID;",
            rows);
    record(q5);
    if (q5) {
        record(assert_rows_equal("Join result validation", rows, {"Alice 150", "Carol 500"}));
    }

    bool q6 = query_rows(db, "SELECT ORDER_ID FROM TEST_ORDERS WHERE USER_ID = 1 ORDER BY ORDER_ID;", rows);
    record(q6);
    if (q6) {
        record(assert_rows_equal("Single-condition equality WHERE validation", rows, {"101", "102"}));
    }

    bool q7 = query_rows(
            db,
            "SELECT TEST_USERS.NAME, TEST_ORDERS.AMOUNT "
            "FROM TEST_USERS INNER JOIN TEST_ORDERS ON TEST_USERS.ID = TEST_ORDERS.USER_ID "
            "WHERE TEST_ORDERS.AMOUNT > 900;",
            rows);
    record(q7);
    if (q7) {
        record(assert_row_count("Join with no matches validation", rows, 0));
    }

    record(expect_query_failure(db, "SELECT UNKNOWN_COLUMN FROM TEST_USERS;", "Invalid SQL should fail"));
    record(expect_query_failure(db, "SELECT * FROM MISSING_TABLE;", "Missing table should fail"));

    int passed_tests = total_tests - failed_tests;
    cout << "\nUnit Test Summary: " << passed_tests << "/" << total_tests << " passed, "
         << failed_tests << " failed.\n\n";

    return all_ok;
}

static bool run_insert_benchmark(FlexQL *db,
                                 const string &host,
                                 int port,
                                 long long target_rows,
                                 int total_columns,
                                 int batch_size,
                                 int client_threads) {
    const string table_name = "BIG_USERS_I_C" + to_string(total_columns);
    const string insert_prefix = "INSERT INTO " + table_name + " VALUES ";
    const string legacy_table_name = "BIG_USERS_C" + to_string(total_columns);

    if (legacy_table_name != table_name) {
        char *legacy_err = nullptr;
        const string cleanup_sql = "DELETE FROM " + legacy_table_name + ";";
        const int cleanup_rc = flexql_exec(db, cleanup_sql.c_str(), nullptr, nullptr, &legacy_err);
        if (cleanup_rc == FLEXQL_OK) {
            cout << "[INFO] Reclaimed legacy table state: " << legacy_table_name << "\n";
        }
        if (legacy_err) {
            flexql_free(legacy_err);
        }
    }

    string create_sql = "CREATE TABLE IF NOT EXISTS " + table_name +
                        "(ID INT, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE INT, EXPIRES_AT INT";
    for (int i = 5; i < total_columns; ++i) {
        create_sql += ", COL" + to_string(i) + " INT";
    }
    create_sql += ");";

    {
        if (!run_exec_allow_table_exists(db, create_sql, "CREATE TABLE " + table_name)) {
            return false;
        }
    }

    if (!run_exec(db, "DELETE FROM " + table_name + ";", "RESET " + table_name)) {
        return false;
    }

    cout << "\nStarting insertion benchmark for " << target_rows
         << " rows (columns=" << total_columns
         << ", batch=" << batch_size
         << ", clients=" << client_threads << ")...\n";
    auto bench_start = high_resolution_clock::now();

    atomic<long long> inserted{0};
    atomic<bool> failed{false};
    mutex fail_mutex;
    string fail_message;

    auto set_failure = [&](const string &msg) {
        bool expected = false;
        if (failed.compare_exchange_strong(expected, true, memory_order_acq_rel)) {
            lock_guard<mutex> lk(fail_mutex);
            fail_message = msg;
        }
    };

    string static_extra_cols;
    if (total_columns > 5) {
        static_extra_cols.reserve(static_cast<size_t>(total_columns - 5) * 2);
        for (int i = 5; i < total_columns; ++i) {
            static_extra_cols += ",1";
        }
    }

    const string row_suffix = ", 'u', 'e', 1, 1893456000" + static_extra_cols + ")";
    const size_t row_reserve = 64 + row_suffix.size();

    long long progress_step = target_rows / 10;
    if (progress_step <= 0) {
        progress_step = 1;
    }
    long long next_progress = progress_step;

    const int workers = max(1, client_threads);
    vector<thread> threads;
    threads.reserve(static_cast<size_t>(workers));

    for (int worker = 0; worker < workers; ++worker) {
        const long long base = target_rows / workers;
        const long long extra = (worker < (target_rows % workers)) ? 1 : 0;
        const long long rows_for_worker = base + extra;
        const long long start_index = base * worker + min<long long>(worker, target_rows % workers);

        threads.emplace_back([&, rows_for_worker, start_index]() {
            if (rows_for_worker <= 0) {
                return;
            }

            FlexQL *worker_db = nullptr;
            if (flexql_open(host.c_str(), port, &worker_db) != FLEXQL_OK) {
                set_failure("worker failed to connect to FlexQL");
                return;
            }

            long long local_done = 0;
            string batch_query;
            batch_query.reserve(16ULL * 1024ULL * 1024ULL);
            constexpr size_t kMaxQueryBytes = 15ULL * 1024ULL * 1024ULL;
            const size_t estimated_row_chars = 64ULL + static_extra_cols.size();
            while (local_done < rows_for_worker && !failed.load(memory_order_acquire)) {
                batch_query.clear();
                batch_query.append(insert_prefix);

                int rows_in_stmt = 0;
                while (rows_in_stmt < batch_size && local_done < rows_for_worker) {
                    if (rows_in_stmt > 0 && batch_query.size() + estimated_row_chars > kMaxQueryBytes) {
                        break;
                    }

                    if (rows_in_stmt > 0) {
                        batch_query.push_back(',');
                    }

                    batch_query.push_back('(');
                    const long long id = start_index + local_done + 1;
                    char id_buf[32];
                    char* id_end = append_u64(id_buf, static_cast<unsigned long long>(id));
                    batch_query.append(id_buf, static_cast<size_t>(id_end - id_buf));
                    batch_query.append(row_suffix);

                    ++local_done;
                    ++rows_in_stmt;
                }

                batch_query.push_back(';');

                char *errMsg = nullptr;
                const int rc = flexql_exec(worker_db, batch_query.c_str(), nullptr, nullptr, &errMsg);
                if (rc != FLEXQL_OK) {
                    string msg = "INSERT " + table_name + " batch failed";
                    if (errMsg) {
                        msg += ": ";
                        msg += errMsg;
                        flexql_free(errMsg);
                    }
                    set_failure(msg);
                    break;
                }

                inserted.fetch_add(rows_in_stmt, memory_order_relaxed);
            }

            flexql_close(worker_db);
        });
    }

    while (!failed.load(memory_order_acquire)) {
        const long long done = inserted.load(memory_order_relaxed);
        if (done >= next_progress || done >= target_rows) {
            cout << "Progress: " << done << "/" << target_rows << "\n";
            while (done >= next_progress) {
                next_progress += progress_step;
            }
        }
        if (done >= target_rows) {
            break;
        }
        this_thread::sleep_for(chrono::milliseconds(100));
    }

    for (auto &t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }

    if (failed.load(memory_order_acquire)) {
        lock_guard<mutex> lk(fail_mutex);
        cout << "[FAIL] INSERT " << table_name << " run -> "
             << (fail_message.empty() ? "unknown error" : fail_message) << "\n";
        return false;
    }

    if (inserted.load(memory_order_relaxed) < target_rows) {
        cout << "[FAIL] INSERT " << table_name << " run -> incomplete insert count\n";
        return false;
    }

    auto bench_end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed > 0) ? (target_rows * 1000LL / elapsed) : target_rows;

    cout << "[PASS] INSERT benchmark complete\n";
    cout << "Rows inserted: " << target_rows << "\n";
    cout << "Columns: " << total_columns << "\n";
    cout << "Batch size: " << batch_size << "\n";
    cout << "Client threads: " << workers << "\n";
    cout << "Elapsed: " << elapsed << " ms\n";
    cout << "Throughput: " << throughput << " rows/sec\n";

    QueryStats verify_stats;
    char *errMsg = nullptr;
    auto verify_start = high_resolution_clock::now();
    int verify_rc = flexql_exec(db,
                                ("SELECT ID FROM " + table_name + ";").c_str(),
                                count_rows_callback,
                                &verify_stats,
                                &errMsg);
    auto verify_end = high_resolution_clock::now();
    long long verify_elapsed = duration_cast<milliseconds>(verify_end - verify_start).count();

    if (verify_rc != FLEXQL_OK) {
        cout << "[FAIL] Post-insert row-count verification -> " << (errMsg ? errMsg : "unknown error") << "\n";
        if (errMsg) {
            flexql_free(errMsg);
        }
        return false;
    }

    cout << "Verification scan: " << verify_stats.rows << " rows in " << verify_elapsed << " ms\n";
    if (verify_stats.rows != target_rows) {
        cout << "[FAIL] Row-count mismatch: expected " << target_rows
             << ", got " << verify_stats.rows << "\n";
        return false;
    }

    vector<string> probe_rows;
    if (!query_rows(db, "SELECT ID FROM " + table_name + " WHERE ID = 1;", probe_rows)) {
        return false;
    }
    if (!assert_row_count("Probe ID=1 exists", probe_rows, 1)) {
        return false;
    }

    if (!query_rows(db,
                    "SELECT ID FROM " + table_name + " WHERE ID = " + to_string(target_rows) + ";",
                    probe_rows)) {
        return false;
    }
    if (!assert_row_count("Probe last ID exists", probe_rows, 1)) {
        return false;
    }

    cout << "[PASS] Insert correctness verified for " << table_name << "\n";

    return true;
}

int main(int argc, char **argv) {
    FlexQL *db = nullptr;
    BenchOptions opts;
    if (!parse_benchmark_args(argc, argv, &opts)) {
        for (int i = 1; i < argc; ++i) {
            const string arg = argv[i];
            if (arg == "-h" || arg == "--help") {
                return 0;
            }
        }
        return 1;
    }

    if (flexql_open(opts.host.c_str(), opts.port, &db) != FLEXQL_OK) {
        cout << "Cannot open FlexQL at " << opts.host << ":" << opts.port << "\n";
        return 1;
    }

    cout << "Connected to FlexQL at " << opts.host << ":" << opts.port << "\n";

    if (opts.run_unit_tests_only) {
        bool ok = run_data_level_unit_tests(db);
        flexql_close(db);
        return ok ? 0 : 1;
    }

    cout << "Running SQL subset checks plus insertion benchmark...\n";
    cout << "Target insert rows: " << opts.rows << "\n";
    cout << "Columns: " << opts.columns << "\n";
    cout << "Batch size: " << opts.batch_size << "\n";
    cout << "Client threads: " << opts.client_threads << "\n\n";

    if (!run_insert_benchmark(db,
                              opts.host,
                              opts.port,
                              opts.rows,
                              opts.columns,
                              opts.batch_size,
                              opts.client_threads)) {
        flexql_close(db);
        return 1;
    }

    if (!opts.skip_unit_tests) {
        if (!run_data_level_unit_tests(db)) {
            flexql_close(db);
            return 1;
        }
    } else {
        cout << "[INFO] Skipping unit tests as requested (--skip-unit-tests).\n";
    }

    flexql_close(db);
    return 0;
}
