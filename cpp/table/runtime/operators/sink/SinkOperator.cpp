/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "SinkOperator.h"

void SinkOperator::open()
{
    if (userFunction == nullptr) {
        userFunction = new DiscardingSink(description);
    }
}

const char *SinkOperator::getName()
{
    return "SinkOperator";
}

void SinkOperator::processElement(StreamRecord *record)
{
    LOG("SinkOperator::processElement(StreamRecord *record)")
    userFunction->invoke(record, SinkInputValueType::ROW_DATA);
}

void SinkOperator::processBatch(StreamRecord *record)
{
    LOG("SinkOperator::processBatch(StreamRecord *record)")
    userFunction->invoke(record, SinkInputValueType::VEC_BATCH);
}

std::string SinkOperator::getTypeName()
{
    std::string typeName = "SinkOperator";
    typeName.append(__PRETTY_FUNCTION__);
    return typeName;
}