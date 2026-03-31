#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/types.h"

namespace flexql {

enum class StatementType {
    CREATE_TABLE,
    INSERT,
    DELETE,
    SELECT,
    SHOW_TABLES,
    DESCRIBE_TABLE,
    INVALID
};

struct CreateTableStmt {
    std::string table_name;
    std::vector<ColumnDef> columns;
    std::string primary_key;
};

struct InsertStmt {
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::string> values;
    std::vector<std::vector<std::string>> rows;
    bool has_expires_at{false};
    std::int64_t expires_at{0};
    bool has_ttl{false};
    std::int64_t ttl_seconds{0};
};

struct DeleteStmt {
    std::string table_name;
};

struct DescribeTableStmt {
    std::string table_name;
};

struct JoinClause {
    std::string right_table;
    std::string left_expr;
    std::string right_expr;
};

struct SelectStmt {
    std::string table_name;
    std::vector<std::string> columns;  // empty means *
    bool has_where{false};
    Condition where;
    bool has_order_by{false};
    std::string order_by_column;
    bool order_by_desc{false};
    bool has_join{false};
    JoinClause join;
};

struct Statement {
    StatementType type{StatementType::INVALID};
    CreateTableStmt create_table;
    InsertStmt insert;
    DeleteStmt del;
    DescribeTableStmt describe;
    SelectStmt select;
};

}  // namespace flexql
