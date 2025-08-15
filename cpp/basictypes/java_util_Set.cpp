/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/java_util_Set.h"

Set::Set() = default;

Set::Set(nlohmann::json jsonObj)
{
    std::unordered_set<nlohmann::json> json_set = jsonObj;
    std::unordered_set<nlohmann::json>::iterator it = json_set.begin();
    for (; it != json_set.end(); it++) {
        if ((*it).is_string()) {
            std::string s = *it;
            this->add(s);
        } else {
            throw std::runtime_error("unsupported type when converting nlohmann::json in set");
        }
    }
}

Set::Set(std::unordered_set<std::string> set)
{
    std::unordered_set<std::string>::iterator itset = set.begin();
    for (; itset != set.end(); itset++) {
        this->add(*itset);
    }
}

bool Set::addAll(List *li)
{
    std::list<Object *>::iterator it = li->list.begin();
    bool res = true;
    for (; it != li->list.end(); it++) {
        if (set_.insert(*it).second)
            (*it)->getRefCount();
    }
    return res;
}

bool Set::addAll(Collection * collection)
{
    // todo implement Set::addAll and add libInterfaceRefs
    return false;
}

bool Set::add(Object *obj) {
    bool b = set_.insert(obj).second;
    if (b)
        obj->getRefCount();
    return b;
}

bool Set::add(std::string str)
{
    String *string = new String(str);
    bool b = set_.insert(string).second;
    if (!b)
        string->putRefCount();
    return b;
}

bool Set::remove(Object *obj)
{
    obj->putRefCount();
    return set_.erase(obj) > 0;
}

bool Set::contains(Object *obj) const
{
    return set_.find(obj) != set_.end();
}

bool Set::contains(const std::string &str) const
{
    String *string = new String(str);
    bool b = set_.find(string) != set_.end();
    string->putRefCount();
    return b;
}

size_t Set::size() const
{
    return set_.size();
}

void Set::clear()
{
    set_.clear();
}

Set::SETIterator::SETIterator(SetIterator begin, SetIterator end): current_(begin), end_(end) {
}

bool Set::SETIterator::hasNext()
{
    return current_ != end_;
}

Object *Set::SETIterator::next()
{
    return *(current_++);
}

Set::~Set()
{
    std::unordered_set<Object *, ObjectSetHash, ObjectSetEqual>::iterator it = set_.begin();
    while (it != set_.end()) {
        (*it)->putRefCount();
        it++;
    }
}

java_util_Iterator *Set::iterator()
{
    java_util_Iterator *it = new SETIterator(set_.begin(), set_.end());
    return it;
}

std::vector<Object *> Set::toArray()
{
    return std::vector<Object *>(set_.begin(), set_.end());
}
