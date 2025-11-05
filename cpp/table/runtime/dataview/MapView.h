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
#ifndef FLINK_TNEL_MAPVIEW_H
#define FLINK_TNEL_MAPVIEW_H
#include "DataView.h"
#include <emhash7.hpp>
#include <optional>

template <typename K, typename V>
class MapView : public DataView {
public:
    MapView() {map = new emhash7::HashMap<K, V>();};
    virtual emhash7::HashMap<K, V> *getMap() { return map; }
    void setMap(emhash7::HashMap<K, V> *map) { this->map = map; };

    virtual std::optional<V> get(const std::optional<K>& key)
    {
        auto it = map->find(*key);
        if (it == map->end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    };
    virtual void put(const std::optional<K>& key, const V& value)
    {
        map->emplace(*key, value);
    }
    ~MapView()
    {
        delete map;
    }

private:
    emhash7::HashMap<K, V> *map;
};

#endif // FLINK_TNEL_MAPVIEW_H
