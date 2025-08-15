/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/LongObjectPool.h"

LongObjectPool& LongObjectPool::getInstance()
{
    constexpr int POOL_SIZE = 16;
    thread_local static LongObjectPool instance(POOL_SIZE);
    return instance;
}

LongObjectPool::LongObjectPool(size_t poolSize)
{
    while (poolSize > 0) {
        Long* newLong = new Long();
        newLong->isPool = true;
        newLong->next = head;
        head = newLong;
        poolSize--;
    }
}

void LongObjectPool::capacityExpansion()
{
    size_t size = capacityExpansionNum;
    while (size > 0) {
        Long* newLong = new Long();
        newLong->isPool = true;
        newLong->next = head;
        head = newLong;
        size--;
    }
    this->capacityExpansionNum = (this->capacityExpansionNum << 1);
}

LongObjectPool::~LongObjectPool()
{
    while (head != nullptr) {
        Long* cur = head;
        head = head->next;
        delete cur;
    }
}