/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "TimeStampInserterSinkOperator.h"

void TimeStampInserterSinkOperator::open()
{
    if (userFunction == nullptr) {
        userFunction = new DiscardingSink(description);
    }
}

const char *TimeStampInserterSinkOperator::getName()
{
    return "TimeStampInserterSinkOperator";
}

void TimeStampInserterSinkOperator::processElement(StreamRecord *record)
{
    LOG("SinkOperator::processElement(StreamRecord *record)")
    userFunction->invoke(record, SinkInputValueType::ROW_DATA);
}

void TimeStampInserterSinkOperator::processBatch(StreamRecord *record)
{
    LOG("SinkOperator::processBatch(StreamRecord *record)")
    auto vb = reinterpret_cast<omnistream::VectorBatch *>(record->getValue());
    if (vb != nullptr) {
        LOG("set time")
        int rowCount = vb->GetRowCount();
        for (int i = 0; i < rowCount; i++) {
            vb->setTimestamp(i, vb->GetValueAt<int64_t>(rowTimeIndex, i));
        }
    }
    LOG("finish set time")

    this->output->collect(record);
}

std::string TimeStampInserterSinkOperator::getTypeName()
{
    std::string typeName = "TimeStampInserterSinkOperator";
    typeName.append(__PRETTY_FUNCTION__);
    return typeName;
}