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

#pragma once

#include "common.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/dataview/StateDataViewStore.h"
#include "table/data/JoinedRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
class WindowAggHandleFunction : public NamespaceAggsHandleFunction<int64_t> {
public:
    virtual bool isWindowEmpty() = 0;
};

class CompositeWindowAggFunction : public WindowAggHandleFunction {
public:
    explicit CompositeWindowAggFunction(
            std::vector<std::unique_ptr<WindowAggHandleFunction>> functions, std::vector<int32_t> outputTypesId, SliceAssigner* sliceAssigner)
            :
            functions_(std::move(functions)), 
            outputTypeIds_(std::move(outputTypesId)),
            sliceAssigner(sliceAssigner) {}

    ~CompositeWindowAggFunction() override = default;

    void open(StateDataViewStore* store) override;
    void accumulate(RowData* accInput) override;
    void retract(RowData* retractInput) override;
    void merge(int64_t ns, RowData* otherAcc) override;
    void setAccumulators(int64_t ns, RowData* acc) override;
    RowData* getAccumulators() override;
    RowData* createAccumulators(int accumulatorArity) override;
    RowData* getValue(int64_t ns) override;
    void Cleanup(int64_t ns) override;
    void close() override;
    bool isWindowEmpty() override;

private:
    SliceAssigner* sliceAssigner; 
    std::vector<std::unique_ptr<WindowAggHandleFunction>> functions_;
    std::vector<int32_t> outputTypeIds_;//output id without key
};