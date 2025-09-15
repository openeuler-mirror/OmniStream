/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_DESERIALIZATIONRESULT_H
#define FLINK_TNEL_DESERIALIZATIONRESULT_H

#include <string>

class DeserializationResult {
public:
    DeserializationResult(const bool isFullRecord, const bool isBufferConsumed) :
            whetherFullRecord(isFullRecord), whetherBufferConsumed(isBufferConsumed){ }

    __attribute__((always_inline)) bool isFullRecord() const
    {
        return whetherFullRecord;
    }

    __attribute__((always_inline)) bool isBufferConsumed() const
    {
        return whetherBufferConsumed;
    }

    std::string toDebugString() const;

private:
    const bool whetherFullRecord;
    const bool whetherBufferConsumed;
};

extern DeserializationResult DeserializationResult_PARTIAL_RECORD;
extern DeserializationResult DeserializationResult_INTERMEDIATE_RECORD_FROM_BUFFER;
extern DeserializationResult DeserializationResult_LAST_RECORD_FROM_BUFFER;

#endif  //FLINK_TNEL_DESERIALIZATIONRESULT_H
