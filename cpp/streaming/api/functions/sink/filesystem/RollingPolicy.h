/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_ROLLING_POLICY_H
#define OMNISTREAM_ROLLING_POLICY_H

#include "./FileWriter.h"

template <typename IN, typename BucketID>
class RollingPolicy {
public:
    RollingPolicy(int64_t rollingFileSize_, int64_t rollingTimeInterval_, int64_t inactivityInterval_) : rollingFileSize(rollingFileSize_),
                                                                                                         rollingTimeInterval(rollingTimeInterval_),
                                                                                                         inactivityInterval(inactivityInterval_)
    {
        if (rollingFileSize <= 0 || rollingTimeInterval <= 0 || inactivityInterval <= 0) {
            throw std::invalid_argument("rollingFileSize, rollingTimeInterval and inactivityInterval must be > 0");
        }
    }

    bool shouldRollOnEvent(FileWriter<IN, BucketID> &part, IN element)
    {
        return part.getSize() > rollingFileSize;
    }

    bool shouldRollOnProcessingTime(FileWriter<IN, BucketID> &part, long currentTime)
    {
        return (currentTime - part.getCreationTime() >= rollingTimeInterval) ||
               (currentTime - part.getLastUpdateTime() >= inactivityInterval);
    }

private:
    bool rollOnCheckpoint;
    int64_t rollingFileSize;
    int64_t rollingTimeInterval;
    int64_t inactivityInterval;
};

#endif // OMNISTREAM_ROLLING_POLICY_H