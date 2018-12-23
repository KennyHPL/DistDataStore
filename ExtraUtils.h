#pragma once

#include <unordered_map>

/// Allows inserting / replacing into an unordered_map when the value is not
/// copy-constructible (in which case map[key] = val does not work).
template <typename K, typename V>
void
insertOrReplace(std::unordered_map<K, V> &map, const K &key, const V &val)
{
    auto success = map.emplace(key, val);
    if (!success.second)
        success.first->second = val;
}
