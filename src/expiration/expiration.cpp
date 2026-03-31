#include "expiration/expiration.h"

#include "utils/time_utils.h"

namespace flexql {

std::int64_t ExpirationPolicy::default_expiration() {
    return utils::now_epoch_seconds() + 24 * 60 * 60;
}

bool ExpirationPolicy::is_expired(std::int64_t expires_at) {
    return utils::now_epoch_seconds() >= expires_at;
}

}  // namespace flexql
