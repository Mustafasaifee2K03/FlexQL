#include "cache/lru_cache.h"

namespace flexql {

LRUCache::LRUCache(std::size_t capacity) : capacity_(capacity ? capacity : 1) {}

std::optional<ResultSet> LRUCache::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    entries_.splice(entries_.begin(), entries_, it->second);
    return it->second->second;
}

void LRUCache::put(const std::string& key, const ResultSet& value) {
    std::lock_guard<std::mutex> lock(mutex_);

    const auto it = index_.find(key);
    if (it != index_.end()) {
        it->second->second = value;
        entries_.splice(entries_.begin(), entries_, it->second);
        return;
    }

    entries_.push_front(Entry{key, value});
    index_[key] = entries_.begin();

    if (entries_.size() > capacity_) {
        const auto& evicted = entries_.back().first;
        index_.erase(evicted);
        entries_.pop_back();
    }
}

void LRUCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
    index_.clear();
}

}  // namespace flexql
