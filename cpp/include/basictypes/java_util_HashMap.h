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

#ifndef FLINK_TNEL_HASHMAP_H
#define FLINK_TNEL_HASHMAP_H

#include <unordered_map>
#include "java_util_Map_Entry.h"
#include "Object.h"
#include "String.h"
#include "java_util_Set.h"
#include "nlohmann/json.hpp"
#include "Double.h"
#include "Integer.h"
#include "core/include/emhash7.hpp"

class Set;
//  unordered_map as HashMap
class HashMap : public Object {
public:
    using MapType = emhash7::HashMap<
        Object *,
        Object *
    >;

    MapType* map_ = nullptr;

    HashMap();

    HashMap(MapType *map);

    HashMap(nlohmann::json jsonObj);

    HashMap(emhash7::HashMap<std::string, std::list<std::string>>);

    HashMap(emhash7::HashMap<std::string, std::string>);

    HashMap(emhash7::HashMap<Object*, Object*>*, bool assign);

    explicit HashMap(HashMap *map);

    virtual ~HashMap();

    Object *get(Object *key) const;

    Object *get(const std::string &key) const;

    Object *put(Object *key, Object *value);

    void putAll(HashMap *map);

    Object *put(const std::string &key, Object *value);

    Object *put(const std::string &key, const std::string &value);

    bool containsKey(Object *key) const;

    bool containsKey(const std::string &key) const;

    size_t size() const;

    // todo returnType is different from java
    bool remove(Object *key);

    bool remove(std::string str);

    void clear();

    // Set<MapEntry> HashMap.entrySet(void)
    // Set<K> HashMap.keySet(void)

    Set *entrySet();

    Set *keySet();

    typename MapType::Iterator begin();

    typename MapType::Iterator end();

    Object *clone() override;

    class MapIterator : public java_util_Iterator {
    public:
        using mapIterator = MapType::Iterator;

        mapIterator current_;
        mapIterator lastRet;
        HashMap* map;

    public:
        MapIterator(HashMap* map);

        ~MapIterator();

        bool hasNext();

        void remove();

        Object *next();
    };

    class EmptyIterator : public java_util_Iterator {
    public:
    public:
        EmptyIterator() = default;

        bool hasNext()
        {
            return false;
        }

        Object *next()
        {
            return nullptr;
        }
    };

    // need free
    java_util_Iterator *iterator();
    thread_local static java_util_Iterator *EMPTY_ITERATOR;
};

using Map = HashMap;
#endif // FLINK_TNEL_HASHMAP_H
