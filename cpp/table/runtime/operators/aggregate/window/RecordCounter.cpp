/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "RecordCounter.h"

RetractionRecordCounter::RetractionRecordCounter(int indexOfCountStar_)
    : indexOfCountStar(indexOfCountStar_) {}

bool AccumulationRecordCounter::recordCountIsZero(RowData *acc)
{
    return acc == nullptr;
}

bool RetractionRecordCounter::recordCountIsZero(RowData *acc)
{
    return acc == nullptr || *acc->getLong(indexOfCountStar) == 0;
}

std::unique_ptr<RecordCounter> RecordCounter::of(int indexOfCountStar_)
{
    if (indexOfCountStar_ >= 0) {
        return std::make_unique<RetractionRecordCounter>(indexOfCountStar_);
    } else {
        return std::make_unique<AccumulationRecordCounter>();
    }
}
