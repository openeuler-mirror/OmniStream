/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#ifndef FLINK_TNEL_NONREUSINGDESERIALIZATIONDELEGATE_H
#define FLINK_TNEL_NONREUSINGDESERIALIZATIONDELEGATE_H

#include <memory>
#include "DeserializationDelegate.h"
#include "../typeutils/TypeSerializer.h"

class NonReusingDeserializationDelegate : public DeserializationDelegate {
public:
    explicit NonReusingDeserializationDelegate(std::unique_ptr<TypeSerializer> serializer);
    void* getInstance() override;
    void setInstance(void* instance) override;
    void write(DataOutputSerializer& out) override;
    void read(DataInputView& in) override;
private:
    void* instance_;
    std::unique_ptr<TypeSerializer> serializer_;
};


#endif  //FLINK_TNEL_NONREUSINGDESERIALIZATIONDELEGATE_H
