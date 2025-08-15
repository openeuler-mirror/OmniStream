/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_PROCESSINGTIMESERVICEUTIL_H
#define FLINK_TNEL_PROCESSINGTIMESERVICEUTIL_H

#include <atomic>

class ProcessingTimeServiceUtil {
public:
    static int64_t getProcessingTimeDelay(int64_t processingTimestamp, int64_t currentTimestamp)
    {
        if (processingTimestamp >= currentTimestamp) {
            return processingTimestamp - currentTimestamp + 1;
        } else {
            return 0;
        }
    }
};
#endif // FLINK_TNEL_PROCESSINGTIMESERVICEUTIL_H
