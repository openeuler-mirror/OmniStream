/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_SINKFUNCTION_H
#define FLINK_TNEL_SINKFUNCTION_H

#include "table/data/RowData.h"
#include "functions/Watermark.h"
enum SinkInputValueType {
    ROW_DATA = 1,
    VEC_BATCH = 2
};
template <typename IN>
class SinkFunction {
public:
    virtual void invoke(IN value, SinkInputValueType valueType) = 0;
    virtual void writeWatermark(Watermark *watermark) = 0;
    virtual void finish() = 0;
};

#endif // FLINK_TNEL_SINKFUNCTION_H
