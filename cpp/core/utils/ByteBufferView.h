/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_BYTEBUFFERVIEW_H
#define FLINK_TNEL_BYTEBUFFERVIEW_H

#include "ByteBuffer.h"

class ByteBufferView :  public ByteBuffer {
public:
    static ByteBufferView* wrap(std::vector<uint8_t>* data_);

private:
    ByteBufferView();

    virtual ~ByteBufferView();
};

#endif //FLINK_TNEL_BYTEBUFFERVIEW_H


