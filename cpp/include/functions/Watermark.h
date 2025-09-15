/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_WATERMARK_H
#define FLINK_TNEL_WATERMARK_H

#include <cstdint>
#include <limits.h>
#include "StreamElement.h"

class Watermark : public StreamElement {
public:
    static const Watermark MAX_WATERMARK;

    explicit Watermark(int64_t timestamp);

    int64_t getTimestamp() const;

    void setTimestamp(int64_t timestamp);

private:
    int64_t  timestamp_;
};


#endif //FLINK_TNEL_WATERMARK_H
