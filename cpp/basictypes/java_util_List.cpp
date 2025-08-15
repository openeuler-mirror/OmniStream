/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/java_util_List.h"

Object *List::get(int32_t idx)
{
    auto it = list.begin();
    for (int32_t i = 0; i < idx; ++i) {
        ++it;
    }
    return *it;
}

List::List() = default;

void List::clear()
{
    list.clear();
}

List::List(nlohmann::json jsonObj)
{
    std::list<nlohmann::json> json_list = jsonObj;
    std::list<nlohmann::json>::iterator it = json_list.begin();
    for (; it != json_list.end(); it++) {
        if ((*it).is_string()) {
            std::string s = *it;
            this->add(s);
        } else {
            throw std::runtime_error("unsupported type when converting nlohmann::json in list");
        }
    }
}

List::List(std::list<std::string> list)
{
    std::list<std::string>::iterator it =  list.begin();
    for (; it != list.end(); it++) {
        this->add(*it);
    }
}

void List::add(Object *a)
{
    a->getRefCount();
    list.push_back(a);
}

void List::add(std::string &str)
{
    String *string = new String(str);
    list.push_back((Object *) string);
}

void List::addLast(Object *a)
{
    a->getRefCount();
    list.push_back(a);
}

void List::addFirst(Object *a)
{
    a->getRefCount();
    list.push_front(a);
}

void List::remove(int32_t idx)
{
    Object *obj = this->get(idx);

    this->list.remove(obj);

    obj->putRefCount();
}

bool List::contains(std::string &str)
{
    std::list<Object *>::iterator it = list.begin();
    for (; it != list.end(); it++) {
        String *node = (String *) (*it);
        if (node->equals(str))
            return true;
    }
    return false;
}

bool List::contains(Object *a)
{
    std::list<Object *>::iterator it = list.begin();
    for (; it != list.end(); it++) {
        if ((*it)->equals(a))
            return true;
    }
    return false;
}

int List::size()
{
    return list.size();
}

bool List::isEmpty()
{
    return list.empty();
}

Object *List::getFirst()
{
    return list.front();
}

Object *List::getLast()
{
    return list.back();
}

Object *List::clone()
{
    List *new_list = new List();

    for (auto &i: list) {
        new_list->add(i->clone());
    }

    return new_list;
}

List::ListIterator::ListIterator(listIterator begin, listIterator end)
{
    current_ = begin;
    end_ = end;
}

bool List::ListIterator::hasNext()
{
    return current_ != end_;
}

Object *List::ListIterator::next()
{
    return *(current_++);
}

List::~List()
{
    auto it = list.begin();
    while (it != list.end()) {
        (*it)->putRefCount();
        ++it;
    }
}

java_util_Iterator *List::iterator()
{
    auto *s_iterator = new ListIterator(list.begin(), list.end());
    return s_iterator;
}
