/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_IOREADABLEWRITABLE_H
#define FLINK_TNEL_IOREADABLEWRITABLE_H

#include "DataInputView.h"
#include "DataOutputSerializer.h"

class IOReadableWritable {
public:
    virtual void write(DataOutputSerializer& out) = 0;
    virtual void read(DataInputView& in) = 0;
    virtual ~IOReadableWritable() = default;
};


#endif  //FLINK_TNEL_IOREADABLEWRITABLE_H
