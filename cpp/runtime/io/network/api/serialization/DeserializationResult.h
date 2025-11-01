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

#ifndef FLINK_TNEL_DESERIALIZATIONRESULT_H
#define FLINK_TNEL_DESERIALIZATIONRESULT_H

#include <string>

class DeserializationResult {
public:
    DeserializationResult(const bool isFullRecord, const bool isBufferConsumed)
        : whetherFullRecord(isFullRecord), whetherBufferConsumed(isBufferConsumed){ }

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

#endif
