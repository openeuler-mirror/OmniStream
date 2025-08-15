/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_PROCESSINGTIMECALLBACK_H
#define FLINK_TNEL_PROCESSINGTIMECALLBACK_H

#include <cstdint>

class ProcessingTimeCallback {
public:
    virtual void OnProcessingTime(int64_t timestamp) = 0;
    virtual ~ProcessingTimeCallback() = default;
};
#endif // FLINK_TNEL_PROCESSINGTIMECALLBACK_H
