/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_EXECUTIONCONFIG_H
#define OMNISTREAM_EXECUTIONCONFIG_H

class ExecutionConfig {
public:
    ExecutionConfig(long autoWatermarkInterval) : autoWatermarkInterval(autoWatermarkInterval)
    {
    }

    long getAutoWatermarkInterval()
    {
        return autoWatermarkInterval;
    }

private:
    long autoWatermarkInterval;
};

#endif // OMNISTREAM_EXECUTIONCONFIG_H

