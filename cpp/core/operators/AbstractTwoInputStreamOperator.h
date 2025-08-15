/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by xichen on 9/23/24.
//

#ifndef FLINK_TNEL_ABSTRACTTWOINPUTSTREAMOPERATOR_H
#define FLINK_TNEL_ABSTRACTTWOINPUTSTREAMOPERATOR_H

#include "StreamOperator.h"
#include "../streamrecord/StreamRecord.h"
#include "../include/common.h"


class AbstractTwoInputStreamOperator : public  StreamOperator {
public:
    ~AbstractTwoInputStreamOperator() override = default;

    virtual void processBatch1(StreamRecord* element)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    virtual void processBatch2(StreamRecord* element)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    virtual void processElement1(StreamRecord* element) {
      THROW_LOGIC_EXCEPTION("To be implemented");
    }

    virtual void processElement2(StreamRecord* element) {
       THROW_LOGIC_EXCEPTION("To be implemented");
    }
    virtual void ProcessWatermark1(Watermark* watermark)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
    virtual void ProcessWatermark2(Watermark* watermark)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    std::string getTypeName() override {
        NOT_IMPL_EXCEPTION;
    }
};
#endif  //FLINK_TNEL_ABSTRACTTWOINPUTSTREAMOPERATOR_H
