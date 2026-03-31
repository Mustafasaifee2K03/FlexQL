#pragma once

#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "common/types.h"

namespace flexql {

class LRUCache {
public:
    explicit LRUCache(std::size_t capacity);

    std::optional<ResultSet> get(const std::string& key);
    void put(const std::string& key, const ResultSet& value);
    void clear();

private:
    using Entry = std::pair<std::string, ResultSet>;

    std::size_t capacity_;
    std::list<Entry> entries_;
    std::unordered_map<std::string, std::list<Entry>::iterator> index_;
    std::mutex mutex_;
};

}  // namespace flexql
