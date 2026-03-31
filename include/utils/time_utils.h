#pragma once

#include <cstdint>
#include <string>

namespace flexql::utils {

int64_t now_epoch_seconds();
bool parse_datetime_to_epoch(const std::string& dt, int64_t* out_epoch);

}  // namespace flexql::utils
