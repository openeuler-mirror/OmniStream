/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
