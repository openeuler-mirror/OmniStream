/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_INTERNALWATERMARK_H
#define FLINK_TNEL_INTERNALWATERMARK_H

#include <cstdint>
#include "functions/Watermark.h"

class InternalWatermark : public Watermark
{
public:
    InternalWatermark(int64_t timestamp) : Watermark(timestamp) {};
private:
    int32_t subpartitionIndex;
};

#endif // FLINK_TNEL_INTERNALWATERMARK_H
