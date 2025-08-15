/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_TIMEWINDOWUTIL_H
#define OMNISTREAM_TIMEWINDOWUTIL_H

#include <cstdint>
#include <climits>

class TimeWindowUtil {
public:
    static bool isWindowFired(int64_t windowEnd, int64_t currentProgress)
    {
        if (windowEnd == LONG_MAX) {
            return false;
        }
        return currentProgress >= windowEnd - 1;
    };
};


#endif // OMNISTREAM_TIMEWINDOWUTIL_H