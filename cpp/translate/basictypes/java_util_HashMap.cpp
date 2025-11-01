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
#include "basictypes/java_util_HashMap.h"

HashMap::HashMap()
{
    map_ = new MapType();
}

HashMap::HashMap(HashMap *map)
{
    map_ = new MapType();
    for (auto &pair: *map->map_) {
        pair.first->getRefCount();
        pair.second->getRefCount();
        (*map_)[pair.first] = pair.second;
    }
}

HashMap::HashMap(MapType *map)
{
    map_ = new MapType();
    for (auto &pair: *map) {
        pair.first->getRefCount();
        pair.second->getRefCount();
        (*map_)[pair.first] = pair.second;
    }
}

HashMap::HashMap(nlohmann::json jsonObj)
{
    map_ = new MapType();
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

HashMap::HashMap(emhash7::HashMap<std::string, std::list<std::string>> map)
{
    map_ = new MapType();
    for (auto &pair: map) {
        String* key = new String(pair.first);
        List* value = new List(pair.second);
        this->put(key, value);
        key->putRefCount();
        value->putRefCount();
    }
}

HashMap::HashMap(emhash7::HashMap<std::string, std::string> map)
{
    map_ = new MapType();
    for (auto &pair: map) {
        String* key = new String(pair.first);
        String* value = new String(pair.second);
        this->put(key, value);
        key->putRefCount();
        value->putRefCount();
    }
}

HashMap::HashMap(emhash7::HashMap<Object*, Object*>* map, bool assign)
{
    if (assign) {
        map_ = map;
    } else {
        map_ = new MapType();
        for (auto &pair: *map) {
            pair.first->getRefCount();
            pair.second->getRefCount();
            this->put(pair.first, pair.second);
        }
    }
}

HashMap::~HashMap()
{
    delete map_;
}

Object *HashMap::get(Object *key) const
{
    auto it = map_->find(key);
    return (it != map_->end()) ? it->second : nullptr;
}

Object *HashMap::get(const std::string &key) const
{
    String *s = new String(key);
    auto it = map_->find(s);
    s->putRefCount();
    return (it != map_->end()) ? it->second : nullptr;

    return nullptr;
}

Object *HashMap::put(Object *key, Object *value)
{
    if (key == nullptr) {
        return nullptr;
    }
    auto it = map_->find(key);
    key->getRefCount();
    if (likely(value != nullptr)) {
        value->getRefCount();
    }
    if (it == map_->end()) {
        (*map_)[key] = value;
        return nullptr;
    }
    Object *k = it->first;
    Object *v = it->second;
    (*map_)[k] = value;
    if (likely(v != nullptr)) {
        v->putRefCount();
    }
    key->putRefCount();
    return nullptr;
}

void HashMap::putAll(HashMap *map)
{
    for (auto &pair: *(map->map_)) {
        put(pair.first, pair.second);
    }
}

Object *HashMap::put(const std::string &key, Object *value)
{
    String *s = new String(key);
    put(s, value);
    s->putRefCount();
    return nullptr;
}

Object *HashMap::put(const std::string &key, const std::string &value)
{
    String *skey = new String(key);
    String *svalue = new String(value);
    put(skey, svalue);
    skey->putRefCount();
    svalue->putRefCount();
    return nullptr;
}

bool HashMap::containsKey(Object *key) const
{
    return map_->find(key) != map_->end();
}

bool HashMap::containsKey(const std::string &key) const
{
    for (auto &pair: *map_) {
        String *s = (String *) pair.first;
        if (s->equals(key))
            return true;
    }
    return false;
}

size_t HashMap::size() const
{
    return map_->size();
}

bool HashMap::remove(Object *key)
{
    auto it = map_->find(key);
    if (it != map_->end()) {
        it->first->putRefCount();
        it->second->putRefCount();
        map_->erase(it);
        return true;
    }
    return false;
}

bool HashMap::remove(std::string str)
{
    String *strtmp = new String(str);
    auto it = map_->find(strtmp);
    if (it != map_->end()) {
        auto key = it->first;
        auto value = it->second;
        map_->erase(it);
        key->putRefCount();
        value->putRefCount();
        strtmp->putRefCount();
        return true;
    }
    strtmp->putRefCount();
    return false;
}

void HashMap::clear()
{
    for (auto iter = map_->begin(); iter != map_->end(); iter++) {
        iter->first->putRefCount();
        iter->second->putRefCount();
    }
    map_->clear();
    return;
}

Set *HashMap::entrySet()
{
    Set *set = new Set();
    for (auto &pair: *map_) {
        MapEntry *entry = new MapEntry(pair.first, pair.second);
        set->add(entry);
        entry->putRefCount();
    }
    return set;
}

Set *HashMap::keySet()
{
    Set *set = new Set();
    for (auto &pair: *map_) {
        set->add(pair.first);
    }
    return set;
}

emhash7::HashMap<Object *, Object *>::Iterator HashMap::begin() { return map_->begin(); }

emhash7::HashMap<Object *, Object *>::Iterator HashMap::end() { return map_->end(); }

Object *HashMap::clone()
{
    HashMap *new_map = new HashMap();
    for (auto &pair: *map_) {
        new_map->put(pair.first->clone(), pair.second->clone());
    }
    return new_map;
}

HashMap::MapIterator::MapIterator(HashMap* map) : map(map)
{
    current_ = map->begin();
}

HashMap::MapIterator::~MapIterator()
{
    map->putRefCount();
}

bool HashMap::MapIterator::hasNext()
{
    return current_ != map->end();
}

Object *HashMap::MapIterator::next()
{
    lastRet = current_;
    return new MapEntry((*(current_)).first, (*(current_++)).second);
}

void HashMap::MapIterator::remove()
{
    map->remove(lastRet->first);
}

java_util_Iterator *HashMap::iterator()
{
    auto *s_iterator = new MapIterator(this);
    this->getRefCount();
    return s_iterator;
}

thread_local java_util_Iterator *HashMap::EMPTY_ITERATOR = new HashMap::EmptyIterator();