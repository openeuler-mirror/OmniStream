/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef OMNISTREAM_LONGOBJECTPOOL_H
#define OMNISTREAM_LONGOBJECTPOOL_H
#include "Long.h"

class LongObjectPool {
public:
    Long* head;
    static LongObjectPool& getInstance();

    int32_t capacityExpansionNum = 16;

public:
    void capacityExpansion();
private:

    explicit LongObjectPool(size_t poolSize);
    ~LongObjectPool();

    LongObjectPool(const LongObjectPool&) = delete;
    LongObjectPool& operator=(const LongObjectPool&) = delete;
};
#endif // OMNISTREAM_LONGOBJECTPOOL_H
