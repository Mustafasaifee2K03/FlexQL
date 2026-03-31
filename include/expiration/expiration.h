#pragma once

#include <cstdint>

namespace flexql {

class ExpirationPolicy {
public:
    static std::int64_t default_expiration();
    static bool is_expired(std::int64_t expires_at);
};

}  // namespace flexql
