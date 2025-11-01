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
#ifndef COUNTWINDOWAGGFUNCTION_H
#define COUNTWINDOWAGGFUNCTION_H

#include <stdexcept>
#include <optional>
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/data/GenericRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/data/TimestampData.h"

class CountWindowAggFunction : public NamespaceAggsHandleFunction<int64_t> {
public:
    CountWindowAggFunction(int aggIdx, int accIndex, int valueIndex, SliceAssigner* sliceAssigner)
        :sliceAssigner(sliceAssigner),
        aggIdx(aggIdx),
        accIndex(accIndex),
        valueIndex(valueIndex){};
    void open(StateDataViewStore* store) override;
    void accumulate(RowData *accInput) override;
    void merge(long ns, RowData *otherAcc) override;
    void setAccumulators(long ns, RowData *acc) override;
    RowData *getAccumulators() override;
    RowData *createAccumulators(int accumulatorArity) override;
    RowData *getValue(long ns) override;
    void retract(RowData *input) override;
    void Cleanup(long ns) override;
    void close() override {}
private:
    SliceAssigner* sliceAssigner; // tumble
    StateDataViewStore* store;

    int64_t namespaceVal;
    bool valueIsNull = false;
    int aggIdx;
    int accIndex;
    int valueIndex;
    long limit;
    long aggValue;
};


#endif
