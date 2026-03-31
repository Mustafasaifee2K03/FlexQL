# FlexQL System Design

This design prioritizes correctness first, then predictable performance for datasets up to around 10 million rows.

## 1. Storage Layout
- Chosen layout: row-major append-only table files.
- Why: INSERT-heavy workloads are faster with sequential appends, and row reconstruction cost is avoided for SELECT *.
- On-disk representation:
  - `data/tables/<table>.schema` stores table metadata and columns.
  - `data/tables/<table>.data` stores one escaped tab-delimited row per line.
  - Internal system column `__expires_at` (epoch seconds) is appended to each stored row.

## 2. Table Representation
- In-memory table metadata:
  - table name
  - vector of columns (`name`, `type`)
  - primary key column index
  - path to schema/data files
- Row representation in execution path: `vector<string>` to minimize copy-heavy typed object conversions.
- Type-aware parsing/comparison is done only when required by WHERE/JOIN predicates.

## 3. Index Structure
- Primary index on primary key implemented as an in-memory hash index:
  - `unordered_map<string, uint64_t>` maps primary key value -> row id (line number in data file).
- Why hash index:
  - O(1) average point lookup for primary-key WHERE equality.
  - Simpler and faster for assignment constraints than implementing a full B+Tree.
- Index is rebuilt from table file on startup/load and updated on each insert.

## 4. Cache Design
- Thread-safe LRU cache for query results.
- Key: normalized SQL string.
- Value: materialized result set (column names + rows).
- Cache invalidation policy:
  - write operation (`INSERT`, `CREATE`) clears cache entries for correctness.
- Bounded capacity prevents unbounded memory growth.

## 5. Expiration Handling
- Every stored row includes `__expires_at` epoch seconds.
- Reads ignore rows where `now >= __expires_at`.
- Expired rows remain on disk (lazy expiration) to keep insert path fast.
- Optional periodic vacuum can be added later, but correctness does not depend on it.

## 6. Query Execution Flow
1. Parse SQL into AST for supported subset only.
2. Planner/executor chooses operation path:
   - `CREATE`: persist schema and register table metadata.
   - `INSERT`: validate schema/types, append row with expiration, update primary index.
   - `SELECT`: load/scan table, apply single WHERE predicate if present, project columns.
   - `JOIN`: nested-loop join with selective filtering and projection.
3. Build tabular result set.
4. Return rows to client callback (API layer) or REPL printer.

## 7. Concurrency Model
- Multithreaded TCP server with fixed-size thread pool.
- One acceptor thread accepts sockets and enqueues jobs.
- Worker threads parse+execute per request.
- Synchronization:
  - per-table `shared_mutex`: shared lock for SELECT, unique lock for INSERT/CREATE metadata updates.
  - global mutex for table registry creation path.
  - cache has its own mutex.
- This model supports concurrent readers and safe writers without race conditions.

## 8. Network Protocol
- TCP, length-prefixed text payload for robustness.
- Client request frame: `uint32 length` + SQL bytes.
- Server response frame: `uint32 length` + text payload.
- Response payload format:
  - `OK\n` for non-row success.
  - `ERR\n<message>\n` for failures.
  - `RESULT\n<col_count>\n<col1>\t<col2>...\n<row1>\n<row2>...\nEND\n` for SELECT/JOIN.
- Escaping utilities ensure tabs/newlines are serialized safely.

## Supported SQL Subset Enforced
- `CREATE TABLE`
- `INSERT INTO`
- `SELECT` (all/specific columns)
- `SELECT ... WHERE` (single condition only)
- `SELECT ... INNER JOIN ... ON ...` (optional single WHERE)

Unsupported syntax intentionally returns parser error.
