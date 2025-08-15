/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 9/12/24.
//

#include "MemorySegmentFactory.h"

MemorySegment *MemorySegmentFactory::wrap(uint8_t *buffer, int size)
{
    return new MemorySegment(buffer, size);
}
