/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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

struct HashFunction {
    size_t operator()(Object *obj) const {
        return obj ? obj->hashCode() : 0;
    }
};

struct EqualFunction {
    bool operator()(Object *a, Object *b) const {
        if (a == b) return true;
        if (!a || !b) return false;
        return a->equals(b);
    }
};

class Set;
//  unordered_map as HashMap
class HashMap : public Object {
public:
    using MapType = std::unordered_map<
        Object *,
        Object *,
        HashFunction,
        EqualFunction
    >;

    MapType map_;

    HashMap();

    HashMap(nlohmann::json jsonObj);

    HashMap(std::unordered_map<std::string,std::list<std::string>>);

    HashMap(std::unordered_map<std::string, std::string>);

    explicit HashMap(HashMap *map);

    ~HashMap();

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

    // Set<MapEntry> HashMap.entrySet(void)
    // Set<K> HashMap.keySet(void)

    Set *entrySet();

    Set *keySet();

    typename MapType::iterator begin();

    typename MapType::iterator end();

    Object *clone() override;
};

using Map = HashMap;


#endif //FLINK_TNEL_HASHMAP_H
