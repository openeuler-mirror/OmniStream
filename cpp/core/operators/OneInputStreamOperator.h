/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#ifndef FLINK_TNEL_ONEINPUTSTREAMOPERATOR_H
#define FLINK_TNEL_ONEINPUTSTREAMOPERATOR_H

#include "Input.h"
#include "StreamOperator.h"
#include "../include/common.h"

class OneInputStreamOperator : public  StreamOperator, public Input {
public:

    ~OneInputStreamOperator() override = default;

    const char *getName() override {
        // NOT IMPLEMENTED
        return "OneInputStreamOperator";
    }

    std::string getTypeName() override {
        return "OneInputStreamOperator";
    }

    void processBatch(StreamRecord* element) override
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    void processElement(StreamRecord* element) override{
        THROW_LOGIC_EXCEPTION("To be implemented");
    }

    virtual void ProcessWatermark(Watermark* watermark)
    {
        THROW_LOGIC_EXCEPTION("To be implemented");
    }
};


#endif  //FLINK_TNEL_ONEINPUTSTREAMOPERATOR_H
