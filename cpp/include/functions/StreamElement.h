/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/11/24.
//

#ifndef FLINK_TNEL_STREAMELEMENT_H
#define FLINK_TNEL_STREAMELEMENT_H

#include "basictypes/Object.h"

enum class StreamElementTag {
    // the integer value_ for each tag should be exact same with flink Stream element_ tag
    // see StreamElementSerializer
    TAG_UNKNOWN = -1,
    TAG_REC_WITH_TIMESTAMP = 0,
    TAG_REC_WITHOUT_TIMESTAMP = 1,
    TAG_WATERMARK = 2,
    TAG_LATENCY_MARKER = 3,
    TAG_STREAM_STATUS = 4,
    TAG_RECORD_ATTRIBUTES = 5,
    TAG_INTERNAL_WATERMARK = 6,
    VECTOR_BATCH = 127
};

class StreamElement : public Object {
public:
    explicit StreamElement(StreamElementTag tag = StreamElementTag::TAG_UNKNOWN) : tag_(tag)  {};
    virtual ~StreamElement() = default;

    StreamElementTag getTag() const
    {
        return tag_;
    }
    void setTag(StreamElementTag tag)
    {
        tag_ = tag;
    }

private:
    StreamElementTag tag_;
};


#endif //FLINK_TNEL_STREAMELEMENT_H
