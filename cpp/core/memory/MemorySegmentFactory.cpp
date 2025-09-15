/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "MemorySegmentFactory.h"

MemorySegment *MemorySegmentFactory::wrap(uint8_t *buffer, int size)
{
    return new MemorySegment(buffer, size);
}
