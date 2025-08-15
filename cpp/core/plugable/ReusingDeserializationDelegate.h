/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/13/24.
//

#ifndef FLINK_TNEL_REUSINGDESERIALIZATIONDELEGATE_H
#define FLINK_TNEL_REUSINGDESERIALIZATIONDELEGATE_H


#include "DeserializationDelegate.h"
#include "../typeutils/TypeSerializer.h"

class ReusingDeserializationDelegate : public DeserializationDelegate {
public:

    void* getInstance() override;
    void setInstance(void* instance) override;
    void write(DataOutputSerializer& out) override;
    void read(DataInputView& in) override;

private:
    void* instance_;
    TypeSerializer* serializer_;
};


#endif //FLINK_TNEL_REUSINGDESERIALIZATIONDELEGATE_H

