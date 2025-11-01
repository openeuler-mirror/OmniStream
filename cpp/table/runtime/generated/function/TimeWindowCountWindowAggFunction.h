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

#ifndef OMNISTREAM_TIMEWINDOWCOUNTWINDOWAGGFUNCTION_H
#define OMNISTREAM_TIMEWINDOWCOUNTWINDOWAGGFUNCTION_H

#include <stdexcept>
#include <optional>
#include "table/data/GenericRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/data/TimestampData.h"
#include "table/runtime/operators/window/TimeWindow.h"

class TimeWindowCountWindowAggFunction : public NamespaceAggsHandleFunction<TimeWindow> {
public:
    TimeWindowCountWindowAggFunction(int aggIdx, int accIndex, int valueIndex)
        : aggIdx(aggIdx),
          accIndex(accIndex),
          valueIndex(valueIndex) {
    };
    void open(StateDataViewStore* store) override;
    void accumulate(RowData *accInput) override;
    void merge(TimeWindow ns, RowData *otherAcc) override;
    void setAccumulators(TimeWindow ns, RowData *acc) override;
    RowData *getAccumulators() override;
    RowData *createAccumulators(int accumulatorArity) override;
    RowData *getValue(TimeWindow ns) override;
    void retract(RowData *input) override;
    void Cleanup(TimeWindow ns) override;
    void close() override {}

private:
    StateDataViewStore* store;
    TimeWindow namespaceVal;
    bool valueIsNull = false;
    int aggIdx;
    int accIndex;
    int valueIndex;
    long limit;
    long aggValue;
};


#endif
