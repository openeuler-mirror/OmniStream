/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// java_util_List
#ifndef JAVA_UNTIL_LIST_H
#define JAVA_UNTIL_LIST_H

#include <string>
#include <list>
#include "java_util_Iterator.h"
#include "Object.h"
#include "String.h"
#include "nlohmann/json.hpp"
#include "java_util_Collection.h"
#include "Integer.h"
#include "Double.h"

// todo 不同list子类需要分离

class List : public Collection {
public:
    std::list<Object *> list;

    List();

    List(nlohmann::json jsonObj);

    List(std::list<std::string> list);

public:
    Object *get(int32_t idx);

    void clear();

    void add(Object *a);

    void add(std::string &str);

    void addLast(Object *a);

    void addFirst(Object *a);

    void remove(int32_t idx);

    bool contains(std::string &str);

    bool contains(Object *a);

    int size();

    //    void clear() {
    //    }
    bool isEmpty();

    Object *getFirst();

    Object *getLast();

    Object *clone() override;

    class ListIterator : public java_util_Iterator {
    public:
        using listIterator = std::list<Object *>::iterator;

        listIterator current_;
        listIterator end_;

    public:
        ListIterator(listIterator begin, listIterator end);

        bool hasNext();

        Object *next();
    };

    ~List() override;

    // need free
    java_util_Iterator *iterator();
};

using ArrayList = List;
// add remove contains del  isEmpty size Iterator<E> iterator()
#endif // JAVA_UNTIL_LIST_H
