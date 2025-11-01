/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNISTREAM_STREAMING_FILE_SINK_HELPER_H
#define OMNISTREAM_STREAMING_FILE_SINK_HELPER_H

#include "Buckets.h"
#include "streaming/runtime/tasks/ProcessingTimeService.h"

template <typename IN>
class StreamingFileSinkHelper : public ProcessingTimeCallback {
public:
    StreamingFileSinkHelper(
        Buckets<IN, std::string> *buckets,
        ProcessingTimeService *procTimeService,
        long bucketCheckInterval)
        : bucketCheckInterval(bucketCheckInterval),
          procTimeService(procTimeService),
          buckets(buckets)
    {
        long currentProcessingTime = procTimeService->getCurrentProcessingTime();
        procTimeService->registerTimer(currentProcessingTime + bucketCheckInterval, this);
    }

    ~StreamingFileSinkHelper() override = default;

    void commitUpToCheckpoint(long checkpointId)
    {
        buckets->commitUpToCheckpoint(checkpointId);
    }

    void OnProcessingTime(int64_t timestamp) override
    {
        long currentTime = procTimeService->getCurrentProcessingTime();
        buckets->onProcessingTime(currentTime);
        procTimeService->registerTimer(currentTime + bucketCheckInterval, this);
    }

    void onElement(IN batch, int rowId, long elementTimestamp, long currentWatermark)
    {
        long currentProcessingTime = procTimeService->getCurrentProcessingTime();
        buckets->onElement(batch, rowId, currentProcessingTime, elementTimestamp, currentWatermark);
    }

    void close()
    {
        buckets->close();
    }
private:
    const long bucketCheckInterval;
    ProcessingTimeService *procTimeService;
    Buckets<IN, std::string> *buckets;
    ListState<long> *maxPartCountersState;
};

#endif // OMNISTREAM_STREAMING_FILE_SINK_HELPER_H