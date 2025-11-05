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

#ifndef FLINK_TNEL_COUNTDISTINCTFUNCTION_H
#define FLINK_TNEL_COUNTDISTINCTFUNCTION_H

#include "../AggsHandleFunction.h"
#include "../table/runtime/dataview/StateDataViewStore.h"
#include "../runtime/state/VoidNamespace.h"

using namespace omniruntime::type;

class CountDistinctFunction : public AggsHandleFunction {
public:
    CountDistinctFunction(int aggIdx, std::string inputType, int accIndex, int valueIndex, int aggFuncIndex, int filterIndex)
        : valueIsNull(true), aggIdx(aggIdx), accIndex(accIndex), valueIndex(valueIndex), aggFuncIndex(aggFuncIndex), filterIndex(filterIndex)
    {
        hasFilter = filterIndex != -1;
        typeId = LogicalType::flinkTypeToOmniTypeId(inputType);
    }

    void setWindowSize(int windowSize) override {};
    bool equaliser(BinaryRowData *r1, BinaryRowData *r2) override;
    void open(StateDataViewStore *store);
    void accumulate(RowData *accInput) override;
    void accumulate(omnistream::VectorBatch *input, const std::vector<int> &indices) override;
    void retract(RowData *retractInput) override;
    void retract(omnistream::VectorBatch* input, const std::vector<int>& indices) override;
    void merge(RowData *otherAcc) override;
    void setAccumulators(RowData *acc) override;
    void resetAccumulators() override;
    void getAccumulators(BinaryRowData *accumulators) override;
    void createAccumulators(BinaryRowData *accumulators) override;
    void getValue(BinaryRowData *aggValue) override;
    void cleanup() override {};
    void close() override {};

private:
    long aggCount;
    bool valueIsNull;
    int aggIdx;
    int accIndex;
    int valueIndex;
    bool hasFilter;
    int aggFuncIndex;
    int filterIndex;
    omniruntime::type::DataTypeId typeId;
    StateDataViewStore *store;
    KeyedStateMapViewWithKeysNullable<VoidNamespace, long, long> *distinctMapView;
};


#endif // FLINK_TNEL_COUNTDISTINCTFUNCTION_H
