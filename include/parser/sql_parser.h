#pragma once

#include <string>

#include "common/arena.h"
#include "parser/ast.h"

namespace flexql {

class SQLParser {
public:
    bool parse(const std::string& sql, Statement* stmt, std::string* err) const;

    // Zero-allocation INSERT parser. All transient strings are arena-allocated.
    bool parse_insert_fast(const char* sql, std::size_t len,
                           Arena* arena, FastInsertStmt* stmt,
                           std::string* err) const;

private:
    bool parse_create(const std::string& sql, Statement* stmt, std::string* err) const;
    bool parse_insert(const std::string& sql, Statement* stmt, std::string* err) const;
    bool parse_delete(const std::string& sql, Statement* stmt, std::string* err) const;
    bool parse_select(const std::string& sql, Statement* stmt, std::string* err) const;
    bool parse_show_tables(const std::string& sql, Statement* stmt, std::string* err) const;
    bool parse_describe_table(const std::string& sql, Statement* stmt, std::string* err) const;

    static bool parse_operator(const std::string& op, OpType* out);
};

}  // namespace flexql
