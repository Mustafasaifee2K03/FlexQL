#include "parser/sql_parser.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <regex>

#include "utils/string_utils.h"

namespace flexql {

namespace {

inline bool parser_profile_enabled() {
    static const bool enabled = []() {
        const char* env = std::getenv("FLEXQL_PROFILE");
        return env && *env != '\0' && std::strcmp(env, "0") != 0;
    }();
    return enabled;
}

struct ParserProfile {
    std::atomic<std::uint64_t> parse_insert_fast_calls{0};
    std::atomic<std::uint64_t> parse_insert_fast_ns{0};

    ~ParserProfile() {
        if (!parser_profile_enabled()) {
            return;
        }
        const auto calls = parse_insert_fast_calls.load(std::memory_order_relaxed);
        const auto ns = parse_insert_fast_ns.load(std::memory_order_relaxed);
        const double ms = static_cast<double>(ns) / 1'000'000.0;
        std::cerr << "[PROFILE] parse_insert_fast calls=" << calls
                  << " total_ms=" << ms;
        if (calls > 0) {
            std::cerr << " avg_us=" << (static_cast<double>(ns) / 1000.0 / static_cast<double>(calls));
        }
        std::cerr << "\n";
    }
};

ParserProfile g_parser_profile;

struct ScopedParserProfile {
    bool enabled{false};
    std::chrono::steady_clock::time_point start{};

    ScopedParserProfile()
        : enabled(parser_profile_enabled()),
          start(enabled ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point{}) {}

    ~ScopedParserProfile() {
        if (!enabled) {
            return;
        }
        const auto end = std::chrono::steady_clock::now();
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        g_parser_profile.parse_insert_fast_calls.fetch_add(1, std::memory_order_relaxed);
        g_parser_profile.parse_insert_fast_ns.fetch_add(static_cast<std::uint64_t>(ns), std::memory_order_relaxed);
    }
};

std::vector<std::string> split_csv(const std::string& input) {
    std::vector<std::string> out;
    std::string cur;
    bool in_quote = false;
    char quote_char = '\0';

    for (std::size_t i = 0; i < input.size(); ++i) {
        const char c = input[i];
        if ((c == '\'' || c == '"')) {
            if (!in_quote) {
                in_quote = true;
                quote_char = c;
                continue;
            }
            if (quote_char == c) {
                in_quote = false;
                continue;
            }
        }

        if (c == ',' && !in_quote) {
            out.push_back(utils::trim(cur));
            cur.clear();
            continue;
        }
        cur.push_back(c);
    }
    if (!cur.empty()) {
        out.push_back(utils::trim(cur));
    }

    for (auto& item : out) {
        item = utils::trim(item);
        if (item.size() >= 2 &&
            ((item.front() == '\'' && item.back() == '\'') ||
             (item.front() == '"' && item.back() == '"'))) {
            item = item.substr(1, item.size() - 2);
        }
    }

    return out;
}

std::string strip_trailing_semicolon(std::string s) {
    s = utils::trim(s);
    if (!s.empty() && s.back() == ';') {
        s.pop_back();
    }
    return utils::trim(s);
}

inline std::int64_t fast_parse_int(const char** str) {
    const char* p = *str;
    bool neg = false;
    if (*p == '-') {
        neg = true;
        ++p;
    } else if (*p == '+') {
        ++p;
    }

    std::int64_t val = 0;
    while (*p >= '0' && *p <= '9') {
        val = val * 10 + static_cast<std::int64_t>(*p - '0');
        ++p;
    }
    *str = p;
    return neg ? -val : val;
}

}  // namespace

bool SQLParser::parse(const std::string& sql, Statement* stmt, std::string* err) const {
    if (!stmt) {
        if (err) {
            *err = "statement output is null";
        }
        return false;
    }

    const std::string clean = strip_trailing_semicolon(sql);
    const std::string upper = utils::to_upper(clean);

    if (upper.rfind("CREATE TABLE", 0) == 0) {
        return parse_create(clean, stmt, err);
    }
    if (upper.rfind("INSERT INTO", 0) == 0) {
        return parse_insert(clean, stmt, err);
    }
    if (upper.rfind("DELETE FROM", 0) == 0) {
        return parse_delete(clean, stmt, err);
    }
    if (upper.rfind("SHOW TABLES", 0) == 0) {
        return parse_show_tables(clean, stmt, err);
    }
    if (upper.rfind("DESCRIBE", 0) == 0 || upper.rfind("DESC", 0) == 0) {
        return parse_describe_table(clean, stmt, err);
    }
    if (upper.rfind("SELECT", 0) == 0) {
        return parse_select(clean, stmt, err);
    }

    if (err) {
        *err = "unsupported SQL command";
    }
    return false;
}

bool SQLParser::parse_create(const std::string& sql, Statement* stmt, std::string* err) const {
    const std::string s = utils::trim(sql);
    const std::string upper = utils::to_upper(s);

    if (upper.rfind("CREATE TABLE", 0) != 0) {
        if (err) {
            *err = "invalid CREATE TABLE syntax";
        }
        return false;
    }

    auto is_ident_start = [](char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    };
    auto is_ident_char = [&](char c) {
        return is_ident_start(c) || (c >= '0' && c <= '9');
    };

    std::size_t pos = std::string("CREATE TABLE").size();
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) {
        ++pos;
    }

    // Optional compatibility clause: IF NOT EXISTS
    if (pos + 13 <= s.size()) {
        const std::string maybe_if = utils::to_upper(s.substr(pos, 13));
        if (maybe_if == "IF NOT EXISTS") {
            pos += 13;
            while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) {
                ++pos;
            }
        }
    }

    if (pos >= s.size() || !is_ident_start(s[pos])) {
        if (err) {
            *err = "invalid CREATE TABLE syntax";
        }
        return false;
    }

    const std::size_t name_start = pos;
    while (pos < s.size() && is_ident_char(s[pos])) {
        ++pos;
    }
    const std::string table_name = s.substr(name_start, pos - name_start);

    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) {
        ++pos;
    }
    if (pos >= s.size() || s[pos] != '(') {
        if (err) {
            *err = "invalid CREATE TABLE syntax";
        }
        return false;
    }

    const std::size_t defs_start = pos + 1;
    bool in_quote = false;
    char quote_char = '\0';
    int depth = 1;
    ++pos;
    for (; pos < s.size(); ++pos) {
        const char c = s[pos];
        if ((c == '\'' || c == '"')) {
            if (!in_quote) {
                in_quote = true;
                quote_char = c;
                continue;
            }
            if (quote_char == c) {
                in_quote = false;
                continue;
            }
        }
        if (in_quote) {
            continue;
        }
        if (c == '(') {
            ++depth;
        } else if (c == ')') {
            --depth;
            if (depth == 0) {
                break;
            }
        }
    }

    if (depth != 0 || pos >= s.size()) {
        if (err) {
            *err = "invalid CREATE TABLE syntax";
        }
        return false;
    }

    const std::string defs_blob = s.substr(defs_start, pos - defs_start);
    ++pos;
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) {
        ++pos;
    }
    if (pos != s.size()) {
        if (err) {
            *err = "invalid CREATE TABLE syntax";
        }
        return false;
    }

    CreateTableStmt create;
    create.table_name = table_name;
    const auto defs = split_csv(defs_blob);

    for (const auto& def_raw : defs) {
        const std::string def = utils::trim(def_raw);
        if (def.empty()) {
            if (err) {
                *err = "invalid column definition in CREATE TABLE";
            }
            return false;
        }

        const std::string def_upper = utils::to_upper(def);
        if (def_upper.rfind("PRIMARY KEY", 0) == 0) {
            const std::size_t l = def.find('(');
            const std::size_t r = def.rfind(')');
            if (l == std::string::npos || r == std::string::npos || r <= l + 1) {
                if (err) {
                    *err = "invalid column definition in CREATE TABLE";
                }
                return false;
            }
            const std::string pk = utils::trim(def.substr(l + 1, r - l - 1));
            if (pk.empty()) {
                if (err) {
                    *err = "invalid column definition in CREATE TABLE";
                }
                return false;
            }
            create.primary_key = pk;
            continue;
        }

        std::size_t sp = 0;
        while (sp < def.size() && def[sp] != ' ' && def[sp] != '\t') {
            ++sp;
        }
        if (sp == 0 || sp >= def.size()) {
            if (err) {
                *err = "invalid column definition in CREATE TABLE";
            }
            return false;
        }

        const std::string col_name = def.substr(0, sp);
        std::string tail = utils::trim(def.substr(sp + 1));
        if (tail.empty()) {
            if (err) {
                *err = "invalid column definition in CREATE TABLE";
            }
            return false;
        }

        const std::string tail_upper = utils::to_upper(tail);
        DataType type = DataType::VARCHAR;
        std::size_t consumed = 0;
        if (tail_upper.rfind("INT", 0) == 0) {
            type = DataType::INT;
            consumed = 3;
        } else if (tail_upper.rfind("DECIMAL", 0) == 0) {
            type = DataType::DECIMAL;
            consumed = 7;
        } else if (tail_upper.rfind("DATETIME", 0) == 0) {
            type = DataType::DATETIME;
            consumed = 8;
        } else if (tail_upper.rfind("VARCHAR", 0) == 0) {
            type = DataType::VARCHAR;
            consumed = 7;
            if (consumed < tail.size() && tail[consumed] == '(') {
                const std::size_t close = tail.find(')', consumed + 1);
                if (close == std::string::npos) {
                    if (err) {
                        *err = "invalid column definition in CREATE TABLE";
                    }
                    return false;
                }
                consumed = close + 1;
            }
        } else {
            if (err) {
                *err = "invalid column definition in CREATE TABLE";
            }
            return false;
        }

        const std::string suffix = utils::trim(tail.substr(consumed));
        const bool is_pk = (!suffix.empty() && utils::to_upper(suffix) == "PRIMARY KEY");
        if (!suffix.empty() && !is_pk) {
            if (err) {
                *err = "invalid column definition in CREATE TABLE";
            }
            return false;
        }

        ColumnDef col{col_name, type, is_pk};
        if (is_pk) {
            create.primary_key = col.name;
        }
        create.columns.push_back(std::move(col));
    }

    if (create.columns.empty()) {
        if (err) {
            *err = "CREATE TABLE requires at least one column";
        }
        return false;
    }

    stmt->type = StatementType::CREATE_TABLE;
    stmt->create_table = std::move(create);
    return true;
}

bool SQLParser::parse_insert(const std::string& sql, Statement* stmt, std::string* err) const {
    const std::string s = utils::trim(sql);
    const std::string upper = utils::to_upper(s);
    if (upper.rfind("INSERT INTO", 0) != 0) {
        if (err) {
            *err = "invalid INSERT syntax";
        }
        return false;
    }

    auto is_ident_start = [](char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    };
    auto is_ident_char = [&](char c) {
        return is_ident_start(c) || (c >= '0' && c <= '9');
    };

    std::size_t pos = std::string("INSERT INTO").size();
    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) {
        ++pos;
    }
    if (pos >= s.size() || !is_ident_start(s[pos])) {
        if (err) {
            *err = "invalid INSERT syntax";
        }
        return false;
    }

    const std::size_t table_start = pos;
    while (pos < s.size() && is_ident_char(s[pos])) {
        ++pos;
    }

    InsertStmt insert;
    insert.table_name = s.substr(table_start, pos - table_start);

    while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) {
        ++pos;
    }

    if (pos < s.size() && s[pos] == '(') {
        const std::size_t col_begin = pos + 1;
        int depth = 1;
        bool in_quote = false;
        char quote_char = '\0';
        ++pos;
        for (; pos < s.size(); ++pos) {
            const char c = s[pos];
            if ((c == '\'' || c == '"')) {
                if (!in_quote) {
                    in_quote = true;
                    quote_char = c;
                    continue;
                }
                if (quote_char == c) {
                    in_quote = false;
                    continue;
                }
            }
            if (in_quote) {
                continue;
            }
            if (c == '(') {
                ++depth;
            } else if (c == ')') {
                --depth;
                if (depth == 0) {
                    break;
                }
            }
        }
        if (depth != 0 || pos >= s.size()) {
            if (err) {
                *err = "invalid INSERT syntax";
            }
            return false;
        }

        insert.columns = split_csv(s.substr(col_begin, pos - col_begin));
        ++pos;
        while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t')) {
            ++pos;
        }
    }

    if (pos + 6 > s.size() || utils::to_upper(s.substr(pos, 6)) != "VALUES") {
        if (err) {
            *err = "invalid INSERT syntax";
        }
        return false;
    }
    pos += 6;

    auto skip_ws = [&](std::size_t* p) {
        while (*p < s.size() && (s[*p] == ' ' || s[*p] == '\t' || s[*p] == '\r' || s[*p] == '\n')) {
            ++(*p);
        }
    };

    skip_ws(&pos);
    std::size_t expected_values = 0;
    while (pos < s.size() && s[pos] == '(') {
        const std::size_t val_begin = pos + 1;
        int depth = 1;
        bool in_quote = false;
        char quote_char = '\0';
        ++pos;
        for (; pos < s.size(); ++pos) {
            const char c = s[pos];
            if ((c == '\'' || c == '"')) {
                if (!in_quote) {
                    in_quote = true;
                    quote_char = c;
                    continue;
                }
                if (quote_char == c) {
                    in_quote = false;
                    continue;
                }
            }
            if (in_quote) {
                continue;
            }
            if (c == '(') {
                ++depth;
            } else if (c == ')') {
                --depth;
                if (depth == 0) {
                    break;
                }
            }
        }

        if (depth != 0 || pos >= s.size()) {
            if (err) {
                *err = "invalid INSERT syntax";
            }
            return false;
        }

        auto row_values = split_csv(s.substr(val_begin, pos - val_begin));
        if (expected_values == 0) {
            expected_values = row_values.size();
        } else if (row_values.size() != expected_values) {
            if (err) {
                *err = "invalid INSERT syntax: inconsistent VALUES tuple width";
            }
            return false;
        }
        insert.rows.push_back(std::move(row_values));

        ++pos;
        skip_ws(&pos);
        if (pos < s.size() && s[pos] == ',') {
            ++pos;
            skip_ws(&pos);
            continue;
        }
        break;
    }

    if (insert.rows.empty()) {
        if (err) {
            *err = "invalid INSERT syntax";
        }
        return false;
    }
    insert.values = insert.rows.front();

    skip_ws(&pos);
    if (pos < s.size()) {
        const std::string suffix = utils::trim(s.substr(pos));
        std::smatch sm;
        static const std::regex exp_re(R"(^EXPIRES\s+([0-9]+)$)", std::regex::icase);
        static const std::regex ttl_re(R"(^TTL\s+([0-9]+)$)", std::regex::icase);
        if (std::regex_match(suffix, sm, exp_re)) {
            insert.has_expires_at = true;
            insert.expires_at = std::stoll(sm[1].str());
        } else if (std::regex_match(suffix, sm, ttl_re)) {
            insert.has_ttl = true;
            insert.ttl_seconds = std::stoll(sm[1].str());
        } else {
            if (err) {
                *err = "invalid INSERT expiration suffix; use EXPIRES <epoch> or TTL <seconds>";
            }
            return false;
        }
    }

    stmt->type = StatementType::INSERT;
    stmt->insert = std::move(insert);
    return true;
}

bool SQLParser::parse_delete(const std::string& sql, Statement* stmt, std::string* err) const {
    static const std::regex re(
        R"(^\s*DELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)\s*$)",
        std::regex::icase);
    std::smatch m;
    if (!std::regex_match(sql, m, re)) {
        if (err) {
            *err = "invalid DELETE syntax";
        }
        return false;
    }

    stmt->type = StatementType::DELETE;
    stmt->del.table_name = m[1].str();
    return true;
}

bool SQLParser::parse_show_tables(const std::string& sql, Statement* stmt, std::string* err) const {
    static const std::regex re(
        R"(^\s*SHOW\s+TABLES\s*$)",
        std::regex::icase);
    std::smatch m;
    if (!std::regex_match(sql, m, re)) {
        if (err) {
            *err = "invalid SHOW TABLES syntax";
        }
        return false;
    }

    stmt->type = StatementType::SHOW_TABLES;
    return true;
}

bool SQLParser::parse_describe_table(const std::string& sql, Statement* stmt, std::string* err) const {
    static const std::regex re(
        R"(^\s*(?:DESCRIBE|DESC)\s+(?:TABLE\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*$)",
        std::regex::icase);

    std::smatch m;
    if (!std::regex_match(sql, m, re)) {
        if (err) {
            *err = "invalid DESCRIBE TABLE syntax";
        }
        return false;
    }

    stmt->type = StatementType::DESCRIBE_TABLE;
    stmt->describe.table_name = m[1].str();
    return true;
}

bool SQLParser::parse_select(const std::string& sql, Statement* stmt, std::string* err) const {
    std::smatch m;

    static const std::regex join_re(
        R"(^\s*SELECT\s+(.*?)\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)\s+INNER\s+JOIN\s+([A-Za-z_][A-Za-z0-9_]*)\s+ON\s+([^\s]+)\s*=\s*([^\s]+)(?:\s+WHERE\s+([A-Za-z0-9_\.]+)\s*(=|!=|<=|>=|<|>)\s*(.+?))?(?:\s+ORDER\s+BY\s+([A-Za-z0-9_\.]+)(?:\s+(ASC|DESC))?)?\s*$)",
        std::regex::icase);

    SelectStmt sel;
    if (std::regex_match(sql, m, join_re)) {
        sel.table_name = m[2].str();
        sel.has_join = true;
        sel.join.right_table = m[3].str();
        sel.join.left_expr = m[4].str();
        sel.join.right_expr = m[5].str();

        const auto cols = utils::trim(m[1].str());
        if (cols != "*") {
            sel.columns = split_csv(cols);
        }

        if (m[6].matched) {
            sel.has_where = true;
            sel.where.column = utils::trim(m[6].str());
            if (!parse_operator(m[7].str(), &sel.where.op)) {
                if (err) {
                    *err = "unsupported WHERE operator";
                }
                return false;
            }
            std::string val = utils::trim(m[8].str());
            if (val.size() >= 2 &&
                ((val.front() == '\'' && val.back() == '\'') ||
                 (val.front() == '"' && val.back() == '"'))) {
                val = val.substr(1, val.size() - 2);
            }
            sel.where.value = std::move(val);
        }

        if (m[9].matched) {
            sel.has_order_by = true;
            sel.order_by_column = utils::trim(m[9].str());
            const auto dir = utils::to_upper(utils::trim(m[10].str()));
            sel.order_by_desc = (dir == "DESC");
        }

        stmt->type = StatementType::SELECT;
        stmt->select = std::move(sel);
        return true;
    }

    static const std::regex simple_re(
        R"(^\s*SELECT\s+(.*?)\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+WHERE\s+([A-Za-z0-9_\.]+)\s*(=|!=|<=|>=|<|>)\s*(.+?))?(?:\s+ORDER\s+BY\s+([A-Za-z0-9_\.]+)(?:\s+(ASC|DESC))?)?\s*$)",
        std::regex::icase);

    if (!std::regex_match(sql, m, simple_re)) {
        if (err) {
            *err = "invalid SELECT syntax";
        }
        return false;
    }

    sel.table_name = m[2].str();
    const auto cols = utils::trim(m[1].str());
    if (cols != "*") {
        sel.columns = split_csv(cols);
    }

    if (m[3].matched) {
        sel.has_where = true;
        sel.where.column = utils::trim(m[3].str());
        if (!parse_operator(m[4].str(), &sel.where.op)) {
            if (err) {
                *err = "unsupported WHERE operator";
            }
            return false;
        }
        std::string val = utils::trim(m[5].str());
        if (val.size() >= 2 &&
            ((val.front() == '\'' && val.back() == '\'') ||
             (val.front() == '"' && val.back() == '"'))) {
            val = val.substr(1, val.size() - 2);
        }
        sel.where.value = std::move(val);
    }

    if (m[6].matched) {
        sel.has_order_by = true;
        sel.order_by_column = utils::trim(m[6].str());
        const auto dir = utils::to_upper(utils::trim(m[7].str()));
        sel.order_by_desc = (dir == "DESC");
    }

    stmt->type = StatementType::SELECT;
    stmt->select = std::move(sel);
    return true;
}

bool SQLParser::parse_operator(const std::string& op, OpType* out) {
    if (!out) {
        return false;
    }
    if (op == "=") {
        *out = OpType::EQ;
        return true;
    }
    if (op == "!=") {
        *out = OpType::NE;
        return true;
    }
    if (op == "<") {
        *out = OpType::LT;
        return true;
    }
    if (op == "<=") {
        *out = OpType::LE;
        return true;
    }
    if (op == ">") {
        *out = OpType::GT;
        return true;
    }
    if (op == ">=") {
        *out = OpType::GE;
        return true;
    }
    return false;
}

// ============================================================================
// Fast-path INSERT parser: zero heap allocations
// ============================================================================

namespace {

inline bool ci_char_eq(char a, char b) {
    return (a | 0x20) == (b | 0x20);
}

inline bool ci_prefix(const char* s, std::size_t slen, const char* prefix, std::size_t plen) {
    if (slen < plen) {
        return false;
    }
    for (std::size_t i = 0; i < plen; ++i) {
        if (!ci_char_eq(s[i], prefix[i])) {
            return false;
        }
    }
    return true;
}

inline void skip_ws(const char*& p, const char* end) {
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) {
        ++p;
    }
}

// Count comma-separated values between open and close (not counting nested parens/quotes)
inline std::uint32_t count_csv_items(const char* start, const char* end) {
    if (start >= end) {
        return 0;
    }
    std::uint32_t count = 1;
    bool in_quote = false;
    char qchar = '\0';
    for (const char* p = start; p < end; ++p) {
        if ((*p == '\'' || *p == '"') && !in_quote) {
            in_quote = true;
            qchar = *p;
        } else if (in_quote && *p == qchar) {
            in_quote = false;
        } else if (*p == ',' && !in_quote) {
            ++count;
        }
    }
    return count;
}

// Parse one CSV item: advances p past the item (and past the comma/end).
// Writes a trimmed, unquoted zero-copy view into out.
inline bool parse_csv_item(const char*& p, const char* end,
                           flexql::Arena* arena, flexql::ArenaStringView* out) {
    (void)arena;

    // Skip leading whitespace
    while (p < end && (*p == ' ' || *p == '\t')) {
        ++p;
    }

    if (p >= end) {
        out->data = nullptr;
        out->length = 0;
        return true;
    }

    const char* item_start = p;
    char qchar = '\0';
    bool is_quoted = false;

    if (*p == '\'' || *p == '"') {
        is_quoted = true;
        qchar = *p;
        ++p;
        item_start = p;  // Skip opening quote
    }

    const char* item_end = nullptr;

    if (is_quoted) {
        // Scan to closing quote
        while (p < end && *p != qchar) {
            ++p;
        }
        item_end = p;
        if (p < end) {
            ++p;  // Skip closing quote
        }
        // Skip whitespace after quote
        while (p < end && (*p == ' ' || *p == '\t')) {
            ++p;
        }
        // Skip comma
        if (p < end && *p == ',') {
            ++p;
        }
    } else {
        // Scan to comma or end
        while (p < end && *p != ',') {
            ++p;
        }
        item_end = p;
        // Trim trailing whitespace
        while (item_end > item_start && (*(item_end - 1) == ' ' || *(item_end - 1) == '\t')) {
            --item_end;
        }
        // Skip comma
        if (p < end && *p == ',') {
            ++p;
        }
    }

    const std::size_t len = static_cast<std::size_t>(item_end - item_start);
    out->data = item_start;
    out->length = static_cast<std::uint32_t>(len);
    return true;
}

}  // namespace

bool SQLParser::parse_insert_fast(const char* sql, std::size_t len,
                                  Arena* arena, FastInsertStmt* stmt,
                                  std::string* err) const {
    ScopedParserProfile prof_scope;
    if (!sql || !arena || !stmt) {
        if (err) {
            *err = "null argument";
        }
        return false;
    }

    const char* p = sql;
    const char* end = sql + len;

    // Strip trailing whitespace and semicolons
    while (end > p && (*(end - 1) == ' ' || *(end - 1) == '\t' ||
                       *(end - 1) == '\r' || *(end - 1) == '\n' || *(end - 1) == ';')) {
        --end;
    }

    skip_ws(p, end);

    // Match "INSERT INTO"
    if (!ci_prefix(p, static_cast<std::size_t>(end - p), "INSERT INTO", 11)) {
        if (err) {
            *err = "not an INSERT statement";
        }
        return false;
    }
    p += 11;
    skip_ws(p, end);

    // Extract table name
    const char* name_start = p;
    while (p < end && *p != ' ' && *p != '\t' && *p != '(') {
        ++p;
    }
    if (p == name_start) {
        if (err) {
            *err = "missing table name";
        }
        return false;
    }

    const std::size_t name_len = static_cast<std::size_t>(p - name_start);
    char* tn = arena->dup(name_start, name_len);
    if (!tn) {
        if (err) {
            *err = "arena exhausted";
        }
        return false;
    }
    stmt->table_name.data = tn;
    stmt->table_name.length = static_cast<std::uint32_t>(name_len);

    skip_ws(p, end);

    // Optional column list: (col1, col2, ...)
    stmt->columns = nullptr;
    stmt->column_count = 0;

    if (p < end && *p == '(') {
        // Could be column list or VALUES list — peek for VALUES keyword after matching paren
        // We need to check if there's a VALUES keyword after this paren group
        const char* paren_start = p + 1;
        int depth = 1;
        const char* scan = paren_start;
        while (scan < end && depth > 0) {
            if (*scan == '(') {
                ++depth;
            } else if (*scan == ')') {
                --depth;
            }
            if (depth > 0) {
                ++scan;
            }
        }

        // scan points to closing paren
        const char* after_paren = scan + 1;
        const char* check = after_paren;
        while (check < end && (*check == ' ' || *check == '\t')) {
            ++check;
        }

        // If next token is VALUES, then current paren is column list
        if (ci_prefix(check, static_cast<std::size_t>(end - check), "VALUES", 6)) {
            // Parse column list
            const char* col_end = scan;
            std::uint32_t count = count_csv_items(paren_start, col_end);
            auto* cols = static_cast<ArenaStringView*>(arena->alloc(count * sizeof(ArenaStringView)));
            if (!cols) {
                if (err) {
                    *err = "arena exhausted";
                }
                return false;
            }

            const char* cp = paren_start;
            for (std::uint32_t i = 0; i < count; ++i) {
                if (!parse_csv_item(cp, col_end, arena, &cols[i])) {
                    if (err) {
                        *err = "arena exhausted parsing columns";
                    }
                    return false;
                }
            }

            stmt->columns = cols;
            stmt->column_count = count;
            p = after_paren;
            skip_ws(p, end);
        }
        // else: paren must be VALUES(...), handled below
    }

    // Match "VALUES"
    if (!ci_prefix(p, static_cast<std::size_t>(end - p), "VALUES", 6)) {
        if (err) {
            *err = "expected VALUES keyword";
        }
        return false;
    }
    p += 6;
    skip_ws(p, end);

    if (p >= end || *p != '(') {
        if (err) {
            *err = "expected '(' after VALUES";
        }
        return false;
    }

    // First pass: validate tuples and count rows/columns.
    const char* values_begin = p;
    std::uint32_t tuple_count = 0;
    std::uint32_t expected_values_per_row = 0;
    const char* suffix_start = nullptr;

    while (p < end) {
        skip_ws(p, end);
        if (p >= end || *p != '(') {
            if (err) {
                *err = "expected VALUES tuple";
            }
            return false;
        }

        ++p;  // skip '(' of tuple
        const char* tuple_start = p;

        int depth = 1;
        bool in_q = false;
        char qc = '\0';
        while (p < end && depth > 0) {
            if ((*p == '\'' || *p == '"') && !in_q) {
                in_q = true;
                qc = *p;
            } else if (in_q && *p == qc) {
                in_q = false;
            } else if (!in_q) {
                if (*p == '(') {
                    ++depth;
                } else if (*p == ')') {
                    --depth;
                }
            }
            if (depth > 0) {
                ++p;
            }
        }

        if (depth != 0 || p > end) {
            if (err) {
                *err = "unterminated VALUES tuple";
            }
            return false;
        }

        const char* tuple_end = p;
        const std::uint32_t tuple_width = count_csv_items(tuple_start, tuple_end);
        if (tuple_count == 0) {
            expected_values_per_row = tuple_width;
        } else if (tuple_width != expected_values_per_row) {
            if (err) {
                *err = "inconsistent VALUES tuple width";
            }
            return false;
        }
        ++tuple_count;

        if (p < end) {
            ++p;  // skip ')'
        }

        skip_ws(p, end);
        if (p < end && *p == ',') {
            ++p;
            continue;
        }

        suffix_start = p;
        break;
    }

    if (tuple_count == 0 || expected_values_per_row == 0) {
        if (err) {
            *err = "empty VALUES list";
        }
        return false;
    }

    const std::uint64_t total_items =
        static_cast<std::uint64_t>(tuple_count) * static_cast<std::uint64_t>(expected_values_per_row);
    if (total_items > static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max())) {
        if (err) {
            *err = "VALUES list is too large";
        }
        return false;
    }

    auto* vals = static_cast<ArenaStringView*>(
        arena->alloc(static_cast<std::size_t>(total_items) * sizeof(ArenaStringView)));
    if (!vals) {
        if (err) {
            *err = "arena exhausted";
        }
        return false;
    }

    // Second pass: parse all tuple values into flattened array.
    const char* parse_p = values_begin;
    std::uint64_t out_idx = 0;
    for (std::uint32_t row = 0; row < tuple_count; ++row) {
        skip_ws(parse_p, end);
        if (parse_p >= end || *parse_p != '(') {
            if (err) {
                *err = "expected VALUES tuple";
            }
            return false;
        }
        ++parse_p;

        int depth = 1;
        bool in_q = false;
        char qc = '\0';
        const char* tuple_start = parse_p;
        while (parse_p < end && depth > 0) {
            if ((*parse_p == '\'' || *parse_p == '"') && !in_q) {
                in_q = true;
                qc = *parse_p;
            } else if (in_q && *parse_p == qc) {
                in_q = false;
            } else if (!in_q) {
                if (*parse_p == '(') {
                    ++depth;
                } else if (*parse_p == ')') {
                    --depth;
                }
            }
            if (depth > 0) {
                ++parse_p;
            }
        }

        const char* tuple_end = parse_p;
        if (depth != 0 || parse_p > end) {
            if (err) {
                *err = "unterminated VALUES tuple";
            }
            return false;
        }

        const char* cell_p = tuple_start;
        for (std::uint32_t col = 0; col < expected_values_per_row; ++col) {
            if (!parse_csv_item(cell_p, tuple_end, arena, &vals[out_idx++])) {
                if (err) {
                    *err = "failed to parse VALUES tuple";
                }
                return false;
            }
        }

        if (parse_p < end) {
            ++parse_p;
        }
        skip_ws(parse_p, end);
        if (parse_p < end && *parse_p == ',') {
            ++parse_p;
        }
    }

    stmt->values = vals;
    stmt->value_count = expected_values_per_row;
    stmt->row_count = tuple_count;

    // Parse optional suffix: TTL <n> or EXPIRES <n>
    stmt->has_expires_at = false;
    stmt->expires_at = 0;
    stmt->has_ttl = false;
    stmt->ttl_seconds = 0;

    if (!suffix_start) {
        suffix_start = p;
    }

    p = suffix_start;
    skip_ws(p, end);
    if (p < end) {
        if (ci_prefix(p, static_cast<std::size_t>(end - p), "EXPIRES", 7)) {
            p += 7;
            skip_ws(p, end);
            const char* num_start = p;
            const std::int64_t val = fast_parse_int(&p);
            if (p == num_start) {
                if (err) {
                    *err = "invalid EXPIRES value";
                }
                return false;
            }
            skip_ws(p, end);
            if (p != end) {
                if (err) {
                    *err = "trailing content after EXPIRES";
                }
                return false;
            }
            stmt->has_expires_at = true;
            stmt->expires_at = val;
        } else if (ci_prefix(p, static_cast<std::size_t>(end - p), "TTL", 3)) {
            p += 3;
            skip_ws(p, end);
            const char* num_start = p;
            const std::int64_t val = fast_parse_int(&p);
            if (p == num_start) {
                if (err) {
                    *err = "invalid TTL value";
                }
                return false;
            }
            skip_ws(p, end);
            if (p != end) {
                if (err) {
                    *err = "trailing content after TTL";
                }
                return false;
            }
            stmt->has_ttl = true;
            stmt->ttl_seconds = val;
        } else {
            if (err) {
                *err = "invalid INSERT suffix; use EXPIRES <epoch> or TTL <seconds>";
            }
            return false;
        }
    }

    return true;
}

}  // namespace flexql
