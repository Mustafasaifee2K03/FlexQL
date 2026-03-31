#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace flexql {

enum class DataType {
    INT,
    DECIMAL,
    VARCHAR,
    DATETIME
};

enum class OpType {
    EQ,
    NE,
    LT,
    LE,
    GT,
    GE
};

struct ColumnDef {
    std::string name;
    DataType type;
    bool is_primary_key{false};
};

struct Condition {
    std::string column;
    OpType op{OpType::EQ};
    std::string value;
};

struct ResultSet {
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;
};

struct ExecResult {
    bool ok{true};
    std::string message;
    ResultSet result_set;
};

}  // namespace flexql
