#pragma once

#include <string>
#include <vector>

namespace flexql::utils {

std::string trim(const std::string& s);
std::string to_upper(std::string s);
std::vector<std::string> split(const std::string& s, char delim);
std::string join(const std::vector<std::string>& parts, char delim);

std::string escape_field(const std::string& field);
std::string unescape_field(const std::string& field);
std::vector<std::string> split_escaped_tab(const std::string& s);

bool iequals(const std::string& a, const std::string& b);

}  // namespace flexql::utils
