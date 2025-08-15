/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/java_util_HashMap.h"

HashMap::HashMap() = default;

HashMap::HashMap(HashMap *map)
{
    for (auto &pair: map->map_) {
        map_[pair.first->clone()] = pair.second->clone();
    }
}

HashMap::HashMap(nlohmann::json jsonObj)
{
    std::unordered_map<nlohmann::json, nlohmann::json> map = jsonObj;
    for (auto &pair: map) {
        nlohmann::json key = pair.first;
        nlohmann::json value = pair.second;

        Object* k = nullptr;
        Object* v = nullptr;

        if (key.is_string()) {
            k = new String(key);
        } else {
            throw std::runtime_error("unsupported type when converting nlohmann::json in hash_key");
        }

        if (value.is_string()) {
            v = new String(value);
        } else if (value.is_array()) {
            v = new List(value);
        } else {
            throw std::runtime_error("unsupported type when converting nlohmann::json in hash_value");
        }
        this->put(k, v);
        k->putRefCount();
        v->putRefCount();
    }
}

HashMap::HashMap(std::unordered_map<std::string, std::list<std::string>> map)
{
    for (auto &pair: map) {
        String* key = new String(pair.first);
        List* value = new List(pair.second);
        this->put(key, value);
        key->putRefCount();
        value->putRefCount();
    }
}

HashMap::HashMap(std::unordered_map<std::string, std::string> map)
{
    for (auto &pair: map) {
        String* key = new String(pair.first);
        String* value = new String(pair.second);
        this->put(key, value);
        key->putRefCount();
        value->putRefCount();
    }
}

HashMap::~HashMap()
{
    for (auto &pair: map_) {
        pair.first->putRefCount();
        pair.second->putRefCount();
    }
}

Object *HashMap::get(Object *key) const
{
    auto it = map_.find(key);
    return (it != map_.end()) ? it->second : nullptr;
}

Object *HashMap::get(const std::string &key) const
{
    String *s = new String(key);
    auto it = map_.find(s);
    s->putRefCount();
    return (it != map_.end()) ? it->second : nullptr;

    //            String *s = (String *)pair.first;
    //        }
    return nullptr;
}

Object *HashMap::put(Object *key, Object *value)
{
    auto it = map_.find(key);
    key->getRefCount();
    value->getRefCount();
    if (it == map_.end()) {
        map_[key] = value;
        return nullptr;
    }
    Object *k = it->first;
    Object *v = it->second;
    map_[k] = value;
    v->putRefCount();
    key->putRefCount();
    return nullptr;
}

void HashMap::putAll(HashMap *map)
{
    for (auto &pair: map->map_) {
        put(pair.first, pair.second);
    }
}

Object *HashMap::put(const std::string &key, Object *value)
{
    String *s = new String(key);
    put(s, value);
    s->putRefCount();
    //            String *s = (String *)pair.first;
    //                map_[pair.first] = value;
    //            }
    //        }
    //        map_[s] = value;
    return nullptr;
}

Object *HashMap::put(const std::string &key, const std::string &value)
{
    String *skey = new String(key);
    String *svalue = new String(value);
    put(skey, svalue);
    skey->putRefCount();
    svalue->putRefCount();
    //            String *s = (String *)pair.first;
    //                map_[pair.first] = svalue;
    //            }
    //        }
    //        map_[s] = svalue;
    return nullptr;
}

bool HashMap::containsKey(Object *key) const
{
    return map_.find(key) != map_.end();
}

bool HashMap::containsKey(const std::string &key) const
{
    for (auto &pair: map_) {
        String *s = (String *) pair.first;
        if (s->equals(key))
            return true;
    }
    return false;
}

size_t HashMap::size() const
{
    return map_.size();
}

bool HashMap::remove(Object *key)
{
    auto it = map_.find(key);
    if (it != map_.end()) {
        map_.erase(key);
        it->first->putRefCount();
        it->second->putRefCount();
        return true;
    }
    return false;
}

bool HashMap::remove(std::string str)
{
    String *strtmp = new String(str);
    auto it = map_.find(strtmp);
    if (it != map_.end()) {
        it->first->putRefCount();
        it->second->putRefCount();
        strtmp->putRefCount();
        return true;
    }
    strtmp->putRefCount();
    return false;
}

Set *HashMap::entrySet()
{
    Set *set = new Set();
    for (auto &pair: map_) {
        MapEntry *entry = new MapEntry(pair.first, pair.second);
        set->add(entry);
        entry->putRefCount();
    }
    return set;
}

Set *HashMap::keySet()
{
    Set *set = new Set();
    for (auto &pair: map_) {
        set->add(pair.first);
    }
    return set;
}

std::unordered_map<Object *, Object *, HashFunction, EqualFunction>::iterator HashMap::begin() { return map_.begin(); }

std::unordered_map<Object *, Object *, HashFunction, EqualFunction>::iterator HashMap::end() { return map_.end(); }

Object *HashMap::clone()
{
    HashMap *new_map = new HashMap();
    for (auto &pair: map_) {
        new_map->put(pair.first->clone(), pair.second->clone());
    }
    return new_map;
}
