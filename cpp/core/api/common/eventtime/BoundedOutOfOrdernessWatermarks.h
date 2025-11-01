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

#ifndef OMNISTREAM_BOUNDEDOUTOFORDERNESSWATERMARKS_H
#define OMNISTREAM_BOUNDEDOUTOFORDERNESSWATERMARKS_H

#include "WatermarkGenerator.h"

class BoundedOutOfOrdernessWatermarks : public WatermarkGenerator {
public:
    explicit BoundedOutOfOrdernessWatermarks(long outOfOrdernessMillis)
        : maxTimestamp(INT64_MIN + outOfOrdernessMillis + 1), outOfOrdernessMillis(outOfOrdernessMillis)
    {
    }

    void OnEvent(void * event, long eventTimestamp, WatermarkOutput* output) override
    {
        maxTimestamp = std::max(maxTimestamp, eventTimestamp);
    }

    void OnPeriodicEmit(WatermarkOutput* output) override
    {
        output->emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
    }

    ~BoundedOutOfOrdernessWatermarks() override = default;

private:
    long maxTimestamp;
    const long outOfOrdernessMillis;
};


#endif // OMNISTREAM_BOUNDEDOUTOFORDERNESSWATERMARKS_H
