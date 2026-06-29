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

#include "FieldGetter.h"
#include "RowData.h"

FieldGetter::FieldGetter(int fieldPos, getFieldByPosFn getFieldByPos)
    :fieldPos_(fieldPos), getFieldByPos_(getFieldByPos) {
}

FieldGetter::FieldGetter(int fieldPos, bool preciseTimestamp)
    : fieldPos_(fieldPos), timestampGetter_(true), preciseTimestamp_(preciseTimestamp) {
}

void *FieldGetter::getFieldOrNull(RowData *row)
{
    if (timestampGetter_) {
        timestampValue_ = preciseTimestamp_
            ? row->getTimestampPrecise(fieldPos_)
            : row->getTimestamp(fieldPos_);
        return &timestampValue_.value();
    }
    return (row->*getFieldByPos_)(fieldPos_);
}
