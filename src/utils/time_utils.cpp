#include "utils/time_utils.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace flexql::utils {

int64_t now_epoch_seconds() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool parse_datetime_to_epoch(const std::string& dt, int64_t* out_epoch) {
    if (!out_epoch) {
        return false;
    }
    std::tm tm{};
    std::istringstream iss(dt);
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (iss.fail()) {
        return false;
    }
    tm.tm_isdst = -1;
    const std::time_t tt = std::mktime(&tm);
    if (tt == static_cast<std::time_t>(-1)) {
        return false;
    }
    *out_epoch = static_cast<int64_t>(tt);
    return true;
}

}  // namespace flexql::utils
