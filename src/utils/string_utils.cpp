#include "utils/string_utils.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace flexql::utils {

std::string trim(const std::string& s) {
    std::size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) {
        ++start;
    }
    std::size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    return s.substr(start, end - start);
}

std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return s;
}

std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        out.push_back(item);
    }
    return out;
}

std::string join(const std::vector<std::string>& parts, char delim) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i) {
            out.push_back(delim);
        }
        out += parts[i];
    }
    return out;
}

std::string escape_field(const std::string& field) {
    std::string out;
    out.reserve(field.size());
    for (char c : field) {
        if (c == '\\' || c == '\t' || c == '\n') {
            out.push_back('\\');
            if (c == '\t') {
                out.push_back('t');
            } else if (c == '\n') {
                out.push_back('n');
            } else {
                out.push_back('\\');
            }
        } else {
            out.push_back(c);
        }
    }
    return out;
}

std::string unescape_field(const std::string& field) {
    std::string out;
    out.reserve(field.size());
    for (std::size_t i = 0; i < field.size(); ++i) {
        if (field[i] == '\\' && i + 1 < field.size()) {
            const char n = field[i + 1];
            if (n == 't') {
                out.push_back('\t');
                ++i;
            } else if (n == 'n') {
                out.push_back('\n');
                ++i;
            } else if (n == '\\') {
                out.push_back('\\');
                ++i;
            } else {
                out.push_back(field[i]);
            }
        } else {
            out.push_back(field[i]);
        }
    }
    return out;
}

std::vector<std::string> split_escaped_tab(const std::string& s) {
    std::vector<std::string> out;
    std::string cur;
    cur.reserve(s.size());
    bool escape = false;
    for (char c : s) {
        if (escape) {
            cur.push_back('\\');
            cur.push_back(c);
            escape = false;
            continue;
        }
        if (c == '\\') {
            escape = true;
            continue;
        }
        if (c == '\t') {
            out.push_back(unescape_field(cur));
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    if (escape) {
        cur.push_back('\\');
    }
    out.push_back(unescape_field(cur));
    return out;
}

bool iequals(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (std::toupper(static_cast<unsigned char>(a[i])) !=
            std::toupper(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

}  // namespace flexql::utils
