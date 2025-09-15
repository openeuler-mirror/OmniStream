/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_DESERIALIZATIONDELEGATE_H
#define FLINK_TNEL_DESERIALIZATIONDELEGATE_H

#include "../io/IOReadableWritable.h"

class DeserializationDelegate : public IOReadableWritable {
public:
    virtual void setInstance(void* instance) = 0;
    virtual void* getInstance() = 0;
    ~DeserializationDelegate() override = default;

};


#endif  //FLINK_TNEL_DESERIALIZATIONDELEGATE_H
