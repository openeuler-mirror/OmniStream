/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_TIMESTAMPASSIGNER_H
#define FLINK_TNEL_TIMESTAMPASSIGNER_H


#include <limits>

class TimestampAssigner {
public:
    static constexpr long NO_TIMESTAMP = std::numeric_limits<long>::min();

    virtual ~TimestampAssigner() = default;

    virtual long ExtractTimestamp(const void* element, long recordTimestamp) = 0;
};

class RecordTimestampAssigner : public TimestampAssigner {
    long ExtractTimestamp(const void* element, long recordTimestamp) override
    {
        return recordTimestamp;
    }
};
#endif // FLINK_TNEL_TIMESTAMPASSIGNER_H
