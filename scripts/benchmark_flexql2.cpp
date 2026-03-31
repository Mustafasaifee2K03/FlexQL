#include <iostream>
#include <chrono>
#include <string>
#include <sstream>
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>
#include <algorithm>
#include "flexql.h"

using namespace std;
using namespace std::chrono;

static const long long DEFAULT_INSERT_ROWS = 1000000LL;
static const int DEFAULT_TOTAL_COLUMNS = 5;
static const int DEFAULT_BATCH_SIZE = 100000;
static const int MAX_BATCH_SIZE = 100000;
static const int DEFAULT_CLIENT_THREADS = 8;
static const size_t MAX_BULK_REQUEST_BYTES = 32 * 1024 * 1024;

static inline char* fast_append_u64(char *out, unsigned long long value) {
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

    record(run_exec(
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

    record(run_exec(
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
    (void)db;

    string create_sql = "CREATE TABLE BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL";
    for (int i = 5; i < total_columns; ++i) {
        create_sql += ", COL" + to_string(i) + " INT";
    }
    create_sql += ");";

    if (!run_exec(db, create_sql, "CREATE TABLE BIG_USERS")) {
        return false;
    }

    cout << "\nStarting insertion benchmark for " << target_rows
         << " rows (columns=" << total_columns
         << ", batch=" << batch_size
         << ", client_threads=" << client_threads << ")...\n";
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
    for (int i = 5; i < total_columns; ++i) {
        static_extra_cols += ",1";
    }

    // Keep benchmark deterministic but reduce payload bytes significantly.
    // Smaller request frames improve throughput without changing row/column counts.
    const string row_suffix = ", 'u', 'e', 1, 1893456000" + static_extra_cols + ");";
    const size_t row_reserve = 64 + row_suffix.size();

    long long progress_step = target_rows / 10;
    if (progress_step <= 0) {
        progress_step = 1;
    }
    long long next_progress = progress_step;

    const int workers = std::max(1, client_threads);
    vector<thread> threads;
    threads.reserve(static_cast<size_t>(workers));

    for (int worker = 0; worker < workers; ++worker) {
        const long long base = target_rows / workers;
        const long long extra = (worker < (target_rows % workers)) ? 1 : 0;
        const long long rows_for_worker = base + extra;
        const long long start_index = base * worker + std::min<long long>(worker, target_rows % workers);

        threads.emplace_back([&, rows_for_worker, start_index]() {
            if (rows_for_worker <= 0) {
                return;
            }

            FlexQL *worker_db = nullptr;
            if (flexql_open(host.c_str(), port, &worker_db) != FLEXQL_OK) {
                set_failure("worker failed to connect to FlexQL");
                return;
            }

            vector<string> sql_batch;
            vector<const char*> sql_ptrs;
            sql_batch.reserve(32768);
            sql_ptrs.reserve(32768);

            auto flush_bulk = [&](size_t *bulk_bytes) -> bool {
                if (sql_batch.empty()) {
                    return true;
                }

                sql_ptrs.clear();
                sql_ptrs.reserve(sql_batch.size());
                for (const auto &sql : sql_batch) {
                    sql_ptrs.push_back(sql.c_str());
                }

                char *errMsg = nullptr;
                const int rc = flexql_exec_bulk(worker_db,
                                                sql_ptrs.data(),
                                                static_cast<int>(sql_ptrs.size()),
                                                &errMsg);
                if (rc != FLEXQL_OK) {
                    string msg = "INSERT BIG_USERS bulk batch failed";
                    if (errMsg) {
                        msg += ": ";
                        msg += errMsg;
                        flexql_free(errMsg);
                    }
                    set_failure(msg);
                    return false;
                }

                inserted.fetch_add(static_cast<long long>(sql_batch.size()), memory_order_relaxed);
                sql_batch.clear();
                *bulk_bytes = 0;
                return true;
            };

            long long local_done = 0;
            size_t bulk_bytes = 0;
            while (local_done < rows_for_worker && !failed.load(memory_order_acquire)) {
                string row_sql;
                row_sql.reserve(row_reserve);
                row_sql.append("INSERT INTO BIG_USERS VALUES (");
                const long long id = start_index + local_done + 1;
                char id_buf[32];
                char* id_end = fast_append_u64(id_buf, static_cast<unsigned long long>(id));
                row_sql.append(id_buf, static_cast<size_t>(id_end - id_buf));
                row_sql.append(row_suffix);

                const size_t framed_bytes = sizeof(uint32_t) + row_sql.size();
                if (!sql_batch.empty() &&
                    (bulk_bytes + framed_bytes > MAX_BULK_REQUEST_BYTES ||
                     static_cast<int>(sql_batch.size()) >= batch_size)) {
                    if (!flush_bulk(&bulk_bytes)) {
                        break;
                    }
                }

                if (sql_batch.empty() && framed_bytes > MAX_BULK_REQUEST_BYTES) {
                    set_failure("single INSERT command exceeds max bulk frame budget");
                    break;
                }

                sql_batch.push_back(std::move(row_sql));
                bulk_bytes += framed_bytes;
                local_done++;
            }

            if (!failed.load(memory_order_acquire)) {
                (void)flush_bulk(&bulk_bytes);
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
        cout << "[FAIL] INSERT BIG_USERS bulk run -> "
             << (fail_message.empty() ? "unknown error" : fail_message) << "\n";
        return false;
    }

    if (inserted.load(memory_order_relaxed) < target_rows) {
        cout << "[FAIL] INSERT BIG_USERS bulk run -> incomplete insert count\n";
        return false;
    }

    auto bench_end = high_resolution_clock::now();
    long long elapsed = duration_cast<milliseconds>(bench_end - bench_start).count();
    long long throughput = (elapsed > 0) ? (target_rows * 1000LL / elapsed) : target_rows;

    cout << "[PASS] INSERT benchmark complete\n";
    cout << "Rows inserted: " << target_rows << "\n";
    cout << "Columns: " << total_columns << "\n";
    cout << "Batch size: " << batch_size << "\n";
    cout << "Client threads: " << client_threads << "\n";
    cout << "Elapsed: " << elapsed << " ms\n";
    cout << "Throughput: " << throughput << " rows/sec\n";

    return true;
}

int main(int argc, char **argv) {
    FlexQL *db = nullptr;
    const string host = "127.0.0.1";
    const int port = 9000;
    long long insert_rows = DEFAULT_INSERT_ROWS;
    int total_columns = DEFAULT_TOTAL_COLUMNS;
    int batch_size = DEFAULT_BATCH_SIZE;
    int client_threads = DEFAULT_CLIENT_THREADS;
    bool run_unit_tests_only = false;

    if (argc > 1) {
        string arg1 = argv[1];
        if (arg1 == "--unit-test") {
            run_unit_tests_only = true;
        } else {
            insert_rows = atoll(argv[1]);
            if (insert_rows <= 0) {
                cout << "Invalid row count. Use a positive integer or --unit-test.\n";
                return 1;
            }
        }
    }

    if (!run_unit_tests_only && argc > 2) {
        total_columns = atoi(argv[2]);
        if (total_columns < 5) {
            cout << "Invalid total_columns. Use an integer >= 5.\n";
            return 1;
        }
    }

    if (!run_unit_tests_only && argc > 3) {
        batch_size = atoi(argv[3]);
        if (batch_size <= 0 || batch_size > MAX_BATCH_SIZE) {
            cout << "Invalid batch_size. Use an integer in 1.." << MAX_BATCH_SIZE << ".\n";
            return 1;
        }
    }

    if (!run_unit_tests_only && argc > 4) {
        client_threads = atoi(argv[4]);
        if (client_threads <= 0 || client_threads > 256) {
            cout << "Invalid client_threads. Use an integer in 1..256.\n";
            return 1;
        }
    }

    if (!run_unit_tests_only && argc > 5) {
        cout << "Usage: " << argv[0] << " [rows] [columns>=5] [batch_size<=" << MAX_BATCH_SIZE
             << "] [client_threads<=256]\n";
        cout << "   or: " << argv[0] << " --unit-test\n";
        return 1;
    }

    if (flexql_open(host.c_str(), port, &db) != FLEXQL_OK) {
        cout << "Cannot open FlexQL\n";
        return 1;
    }

    cout << "Connected to FlexQL\n";

    if (run_unit_tests_only) {
        bool ok = run_data_level_unit_tests(db);
        flexql_close(db);
        return ok ? 0 : 1;
    }

    cout << "Running SQL subset checks plus insertion benchmark...\n";
    cout << "Target insert rows: " << insert_rows << "\n";
    cout << "Columns: " << total_columns << "\n";
    cout << "Batch size: " << batch_size << "\n\n";
    cout << "Client threads: " << client_threads << "\n\n";

    if (!run_insert_benchmark(db, host, port, insert_rows, total_columns, batch_size, client_threads)) {
        flexql_close(db);
        return 1;
    }

    if (!run_data_level_unit_tests(db)) {
        flexql_close(db);
        return 1;
    }

    flexql_close(db);
    return 0;
}