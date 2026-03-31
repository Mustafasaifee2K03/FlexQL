#include <iostream>
#include <string>
#include <cstring>
#include <cassert>
#include "common/flexql.h"

namespace {

int count_callback(void* data, int, char**, char**) {
    int* count = static_cast<int*>(data);
    (*count)++;
    return 0;
}

int print_callback(void* data, int col_count, char** values, char** col_names) {
    for (int i = 0; i < col_count; ++i) {
        std::cout << col_names[i] << "=" << values[i];
        if (i + 1 < col_count) std::cout << " | ";
    }
    std::cout << "\n";

    int* count = static_cast<int*>(data);
    if (count) (*count)++;
    return 0;
}

bool exec_sql(FlexQL* db, const std::string& sql, int* row_count = nullptr, bool print = false) {
    char* errmsg = nullptr;
    int count = 0;
    int* count_ptr = row_count ? row_count : &count;
    *count_ptr = 0;  // Initialize to 0

    const int rc = flexql_exec(db, sql.c_str(),
        print ? print_callback : count_callback,
        count_ptr,
        &errmsg);

    if (rc != FLEXQL_OK) {
        std::cerr << "FAILED: " << sql << "\n";
        std::cerr << "Error: " << (errmsg ? errmsg : "unknown") << "\n";
        if (errmsg) flexql_free(errmsg);
        return false;
    }

    return true;
}

}  // namespace

int main() {
    FlexQL* db = nullptr;
    if (flexql_open("127.0.0.1", 9001, &db) != FLEXQL_OK) {
        std::cerr << "Failed to connect to server\n";
        return 1;
    }

    std::cout << "=== FlexQL Correctness Test ===\n\n";

    // Test 1: CREATE TABLE
    std::cout << "Test 1: CREATE TABLE\n";
    if (!exec_sql(db, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR, age INT)")) {
        return 1;
    }
    std::cout << "  PASS: Created users table\n";

    // Test 2: INSERT rows
    std::cout << "\nTest 2: INSERT\n";
    for (int i = 1; i <= 100; ++i) {
        std::string sql = "INSERT INTO users VALUES (" + std::to_string(i) +
            ", 'user" + std::to_string(i) + "', " + std::to_string(20 + (i % 50)) + ") TTL 86400";
        if (!exec_sql(db, sql)) {
            return 1;
        }
    }
    std::cout << "  PASS: Inserted 100 rows\n";

    // Test 3: SELECT *
    std::cout << "\nTest 3: SELECT * FROM users\n";
    int count = 0;
    if (!exec_sql(db, "SELECT * FROM users", &count)) {
        return 1;
    }
    std::cout << "  PASS: Retrieved " << count << " rows (expected 100)\n";
    if (count != 100) {
        std::cerr << "  FAIL: Expected 100 rows\n";
        return 1;
    }

    // Test 4: SELECT specific columns
    std::cout << "\nTest 4: SELECT specific columns\n";
    count = 0;
    if (!exec_sql(db, "SELECT name, age FROM users", &count)) {
        return 1;
    }
    std::cout << "  PASS: Retrieved " << count << " rows\n";

    // Test 5: SELECT with WHERE on primary key
    std::cout << "\nTest 5: SELECT WHERE on primary key (id = 50)\n";
    count = 0;
    if (!exec_sql(db, "SELECT * FROM users WHERE id = 50", &count, true)) {
        return 1;
    }
    std::cout << "  PASS: Retrieved " << count << " row (expected 1)\n";
    if (count != 1) {
        std::cerr << "  FAIL: Expected 1 row\n";
        return 1;
    }

    // Test 6: SELECT with WHERE on non-PK column
    std::cout << "\nTest 6: SELECT WHERE on non-PK (age = 25)\n";
    count = 0;
    if (!exec_sql(db, "SELECT * FROM users WHERE age = 25", &count)) {
        return 1;
    }
    std::cout << "  PASS: Retrieved " << count << " rows matching age=25\n";

    // Test 7: Create second table for JOIN
    std::cout << "\nTest 7: CREATE second table for JOIN\n";
    if (!exec_sql(db, "CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount DECIMAL)")) {
        return 1;
    }
    std::cout << "  PASS: Created orders table\n";

    // Insert orders
    for (int i = 1; i <= 50; ++i) {
        std::string sql = "INSERT INTO orders VALUES (" + std::to_string(i) +
            ", " + std::to_string((i % 10) + 1) + ", " + std::to_string(100.0 + i * 5.5) + ") TTL 86400";
        if (!exec_sql(db, sql)) {
            return 1;
        }
    }
    std::cout << "  PASS: Inserted 50 orders\n";

    // Test 8: INNER JOIN
    std::cout << "\nTest 8: INNER JOIN (users.id = orders.user_id)\n";
    count = 0;
    if (!exec_sql(db, "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id", &count)) {
        return 1;
    }
    std::cout << "  PASS: JOIN returned " << count << " rows\n";

    // Test 9: INNER JOIN with WHERE
    std::cout << "\nTest 9: INNER JOIN with WHERE\n";
    count = 0;
    if (!exec_sql(db, "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.id = 5", &count, true)) {
        return 1;
    }
    std::cout << "  PASS: JOIN with WHERE returned " << count << " rows\n";

    std::cout << "\n=== All Correctness Tests PASSED ===\n";

    flexql_close(db);
    return 0;
}
