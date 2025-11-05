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

#ifndef FLINK_TNEL_STREAMRECORD_H
#define FLINK_TNEL_STREAMRECORD_H


#include <cstdint>
#include <functional>
#include "StreamElement.h"

class StreamRecord : public StreamElement {
public:
    StreamRecord();
    explicit StreamRecord(void* value);
    StreamRecord(void *value, long timestamp);

    long getTimestamp() const;
    void setTimestamp(long timestamp);
    bool hasTimestamp() const;

    StreamRecord* replace(void* value, long timestamp);
    StreamRecord* replace(void* value);

    void eraseTimestamp();

    bool operator==(const StreamRecord& other) const
    {
        return value_ == other.value_ && timestamp_ == other.timestamp_;
    }

    void setExternalRow(bool hasExternalRow)
    {
        this->hasExternalRow_ = hasExternalRow;
    }

    bool hasExternalRow()
    {
        return this->hasExternalRow_;
    }

private:
    long timestamp_;
    bool hasTimestamp_;
    bool hasExternalRow_ = false;
};

namespace std {
    template <>
    struct hash<StreamRecord> {
        size_t operator()(const StreamRecord& record) const
        {
            // Implement a hash function for StreamRecord
            return std::hash<void*>()(record.getValue());
        }
    };
}

#endif
