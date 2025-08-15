/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by q00649235 on 2025/3/17.
//

#ifndef OMNISTREAM_LRUMAP_H
#define OMNISTREAM_LRUMAP_H

#include <list>
#include <unordered_map>
#include <functional>
#include <optional>
#include <cmath>


// to be used
template<typename K, typename V>
class LRUMap {
public:
    struct DLinkedNode {
        K key;
        V value;
        DLinkedNode* prev;
        DLinkedNode* next;
        DLinkedNode(): key(0), value(0), prev(nullptr), next(nullptr) {}
        DLinkedNode(K _key, V _value): key(_key), value(_value), prev(nullptr), next(nullptr) {}
    };

    explicit LRUMap(int _capacity): capacity(_capacity), size(0)
    {
        // 使用伪头部和伪尾部节点
        head = new DLinkedNode();
        tail = new DLinkedNode();
        head->next = tail;
        tail->prev = head;
        size_t initialCapacity = static_cast<size_t>(std::ceil(_capacity / 0.75)) + 1;
        cache.reserve(initialCapacity);
    }

    std::optional<V> get(K key)
    {
        if (!cache.count(key)) {
            return std::nullopt;
        }
        // 如果 key 存在，先通过哈希表定位，再移到头部
        DLinkedNode* node = cache[key];
        moveToHead(node);
        return node->value;
    }

    void put(K key, V value)
    {
        if (!cache.count(key)) {
            // 如果 key 不存在，创建一个新的节点
            DLinkedNode* node = new DLinkedNode(key, value);
            // 添加进哈希表
            cache[key] = node;
            // 添加至双向链表的头部
            addToHead(node);
            ++size;
            if (size > capacity) {
                // 如果超出容量，删除双向链表的尾部节点
                DLinkedNode* removed = removeTail();
                // 删除哈希表中对应的项
                cache.erase(removed->key);
                // 防止内存泄漏
                delete removed;
                --size;
            }
        } else {
            // 如果 key 存在，先通过哈希表定位，再修改 value，并移到头部
            DLinkedNode* node = cache[key];
            node->value = value;
            moveToHead(node);
        }
    }

    void addToHead(DLinkedNode* node)
    {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }

    void removeNode(DLinkedNode* node)
    {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }

    void moveToHead(DLinkedNode* node)
    {
        removeNode(node);
        addToHead(node);
    }

    DLinkedNode* removeTail()
    {
        DLinkedNode* node = tail->prev;
        removeNode(node);
        return node;
    }

    virtual ~LRUMap()
    {
        delete head;
        delete tail;
    }

private:

    std::unordered_map<K, DLinkedNode*> cache;
    DLinkedNode* head;
    DLinkedNode* tail;
    int size;
    int capacity;
};

#endif
