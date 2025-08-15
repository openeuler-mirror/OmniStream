/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SOURCEOUTPUT_H
#define FLINK_TNEL_SOURCEOUTPUT_H


#include <memory>
#include "api/common/eventtime/WatermarkOutput.h"


class SourceOutput : public WatermarkOutput {
public:
    virtual ~SourceOutput() = default;

    virtual void Collect(void* record) = 0;

    virtual void Collect(void* record, long timestamp) = 0;
};

#endif // FLINK_TNEL_SOURCEOUTPUT_H
