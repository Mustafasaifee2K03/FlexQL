# FlexQL

git clone https://github.com/Mustafasaifee2K03/FlexQL.git

FlexQL is a lightweight SQL-like storage engine with a TCP server and a simple CLI client.

## Requirements (new Linux machine)
- C++17 compiler and make (build-essential on Debian/Ubuntu).
- Optional: libjemalloc-dev only if you build with jemalloc.
- Optional: cmake if you prefer a CMake build.

### Debian/Ubuntu
```bash
sudo apt-get update
sudo apt-get install -y build-essential
# Optional: only needed if you build with jemalloc
sudo apt-get install -y libjemalloc-dev
```

## Build
```bash
make
```
This produces binaries in bin/:
- bin/flexql_server
- bin/flexql_client
- bin/benchmark_flexql (and the other benchmark variants)

Clean:
```bash
make clean
```

## Run the server
```bash
./bin/flexql_server [port] [workers] [data_dir]
```
Defaults: port 9000, workers = hardware threads, data_dir = data/.

## Run the client (manual mode)
```bash
./bin/flexql_client [host] [port]
```
Type SQL and press Enter. Use quit or exit to leave.

Example:
```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64), age INT);
INSERT INTO users VALUES (1, 'Alice', 42);
SELECT * FROM users;
```

## Supported SQL (only)
Statements are case-insensitive. A trailing semicolon is optional.

### CREATE TABLE
```sql
CREATE TABLE [IF NOT EXISTS] table_name (
  col_name TYPE [PRIMARY KEY],
  ...
  [PRIMARY KEY (col_name)]
);
```
Supported types: INT, DECIMAL, DATETIME, VARCHAR or VARCHAR(n).

### INSERT
```sql
INSERT INTO table_name [(col1, col2, ...)]
VALUES (v1, v2, ...)[, (...)]
[EXPIRES <unix_epoch> | TTL <seconds>];
```

### DELETE
```sql
DELETE FROM table_name;
```
Note: DELETE has no WHERE support.

### SELECT (no JOIN)
```sql
SELECT *|col1,col2 FROM table_name
[WHERE col op value]
[ORDER BY col [ASC|DESC]];
```
WHERE operators supported: =, !=, <, <=, >, >=.

### SELECT with INNER JOIN
```sql
SELECT ... FROM left_table
INNER JOIN right_table ON left_table.col = right_table.col
[WHERE col op value]
[ORDER BY col [ASC|DESC]];
```
Note: one side of the ON clause must be a primary key or the query fails.

### SHOW TABLES / DESCRIBE
```sql
SHOW TABLES;
DESCRIBE table_name;
DESC table_name;
```

## Benchmarks
Start the server in one terminal:
```bash
./bin/flexql_server 9000 8 data
```
Run the main benchmark in another terminal:
```bash
./bin/benchmark_flexql --rows 100000 --cols 10 --batch 5000 --clients 8
```

There is also a helper script that builds, launches the server, and runs a benchmark mode:
```bash
./scripts/run_benchmark.sh [rows] [port] [workers] [cols] [batch] [clients] [mode]
```
Modes: original, original2, original-legacy, extreme.

## Make the benchmark big table column a primary key
The large insert benchmark builds its table schema in scripts/benchmark_flexql.cpp.
Edit the CREATE TABLE string at [scripts/benchmark_flexql.cpp](scripts/benchmark_flexql.cpp#L526-L531).

Two equivalent options:
1) Change the ID column to a primary key:
```cpp
"(ID INT PRIMARY KEY, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE INT, EXPIRES_AT INT"
```
2) Or add a table-level primary key before the closing ");":
```cpp
create_sql += ", PRIMARY KEY (ID)";
```
