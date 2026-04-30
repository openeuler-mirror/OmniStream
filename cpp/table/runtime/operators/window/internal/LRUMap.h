/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_LRUMAP_H
#define OMNISTREAM_LRUMAP_H
#include <list>
#include <optional>
#include <emhash7.hpp>

template <typename K, typename V>
class LRUMap {
public:
    LRUMap() {
        cacheMap_.reserve(capacity_);
    };

    explicit LRUMap(size_t cap) : capacity_(cap) {
        if (capacity_ > 0) {
            cacheMap_.reserve(capacity_);
        }
    }

    std::optional<V> get(const K& key)
    {
        auto mapIt = cacheMap_.find(key);
        if (mapIt == cacheMap_.end()) {
            return std::nullopt;
        }
        cacheList_.splice(cacheList_.begin(), cacheList_, mapIt->second.first);
        return mapIt->second.second;
    }

    void put(const K& key, const V& value)
    {
        auto mapIt = cacheMap_.find(key);
        if (mapIt != cacheMap_.end()) {
            cacheList_.splice(cacheList_.begin(), cacheList_, mapIt->second.first);
            mapIt->second.second = value;
        } else {
            if (cacheList_.size() >= capacity_) {
                // delete least recently used element in the end
                K lruKey = cacheList_.back();
                cacheMap_.erase(lruKey);
                cacheList_.pop_back();
            }
            // insert new element at front
            cacheList_.push_front(key);
            cacheMap_[key] = {cacheList_.begin(), value};
        }
    }

private:
    size_t capacity_ = 1024;
    std::list<K> cacheList_;
    emhash7::HashMap<K, std::pair<typename std::list<K>::iterator, V>> cacheMap_;
};

#endif