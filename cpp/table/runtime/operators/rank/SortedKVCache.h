/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_KVCACHE_H
#define OMNISTREAM_KVCACHE_H
#include <iostream>
#include <unordered_map>
#include <list>
#include <memory>

template <typename K, typename V>
class SortedKVCache {
public:
    explicit SortedKVCache() = default;
    explicit SortedKVCache(size_t cap) : capacity(cap) {}
    ~SortedKVCache()
    {
        for (auto& item : cacheList) {
            if constexpr (std::is_pointer<K>::value) {
                delete item.first;
            }
        }
    }

    V get(const K& key)
    {
        if (cacheMap.find(key) == cacheMap.end()) {
            return nullptr;
        }

        cacheList.splice(cacheList.begin(), cacheList, cacheMap[key]);
        return cacheMap[key]->second;
    }

    void put(const K& key, const V& value)
    {
        if (cacheMap.find(key) != cacheMap.end()) {
            cacheList.splice(cacheList.begin(), cacheList, cacheMap[key]);
            if constexpr (std::is_pointer_v<V>) {
                oldValues.push_back(cacheMap[key]->second);
            }
            cacheMap[key]->second = value;
            if constexpr (std::is_pointer<K>::value) {
                delete key;
            }
        } else {
            if (cacheList.size() == capacity) {
                K lruKey = cacheList.back().first;
                cacheMap.erase(lruKey);
                cacheList.pop_back();
            }
            // Insert new element at front
            cacheList.push_front({key, value});
            cacheMap[key] = cacheList.begin();
        }
    }

    void clearOldValues()
    {
        for (auto& value : oldValues) {
            if constexpr (std::is_pointer<V>::value) {
                delete value;
            }
        }
        oldValues.clear();
    }
private:
    size_t capacity = 1024;
    std::list<std::pair<K, V>> cacheList; // Stores key-value pairs
    std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> cacheMap;
    std::vector<V> oldValues;
};

#endif
