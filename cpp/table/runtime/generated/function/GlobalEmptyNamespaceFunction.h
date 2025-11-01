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
#ifndef OMNISTREAM_EMPTYFUNCTION_H
#define OMNISTREAM_EMPTYFUNCTION_H

#include <optional>
#include <stdexcept>
#include <cstdint>
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/data/GenericRowData.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"

class GlobalEmptyNamespaceFunction : public NamespaceAggsHandleFunction<int64_t> {
public:
    GlobalEmptyNamespaceFunction(int aggIdx, int accIndex, int valueIndex, SliceAssigner* sliceAssigner)
        :sliceAssigner(sliceAssigner),
        aggIdx(aggIdx),
        accIndex(accIndex),
        valueIndex(valueIndex){};
    RowData *getValue(int64_t ns);
    void retract(RowData* input) override;
    void open(StateDataViewStore* store) override;
    void setAccumulators(int64_t namespace_val, RowData* accumulators) override;
    void accumulate(RowData* input_row) override;
    void merge(int64_t namespace_val, RowData* other_acc) override;
    RowData *createAccumulators(int accumulatorArity);
    RowData *getAccumulators() override;
    void Cleanup(int64_t namespace_val) override;
    void close() override {};

private:
    SliceAssigner* sliceAssigner; // tumble
    StateDataViewStore* store;
    int64_t namespaceVal;
    bool valueIsNull = false;
    int aggIdx;
    int accIndex = 0;
    int valueIndex = 0;
    long limit;
    long aggValue;
    const int startTimeOffset = 2;
    const int endTimeOffset = 1;
};

#endif
