/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_STREAMRECORD_H
#define FLINK_TNEL_STREAMRECORD_H


#include <cstdint>
#include <functional>
#include "functions/StreamElement.h"


class StreamRecord : public StreamElement {
public:
    StreamRecord();
    explicit StreamRecord(void* value);
    StreamRecord(void *value, long timestamp);

    void *getValue() const;
    void setValue(void *value);
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
    void *value_ = nullptr;
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

#endif  //FLINK_TNEL_STREAMRECORD_H
