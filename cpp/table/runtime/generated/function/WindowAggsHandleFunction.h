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

#include <memory>
#include <utility>
#include <vector>

#include "NamespaceAggsBasicFunction.h"
#include "table/runtime/generated/NamespaceAggsHandleFunction.h"
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "table/runtime/dataview/StateDataViewStore.h"

class WindowAggsHandleFunction : public NamespaceAggsHandleFunction<int64_t> {
public:
    explicit WindowAggsHandleFunction(
        std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> functions,
        std::vector<int32_t> aggValueTypeIds,
        std::vector<int32_t> outputValueTypeIds,
        SliceAssigner* sliceAssigner,
        int32_t accumulatorArity);

    ~WindowAggsHandleFunction() override = default;

    void open(StateDataViewStore* store) override;
    void accumulate(RowData* accInput) override;
    void retract(RowData* retractInput) override;
    void merge(int64_t ns, RowData* otherAcc) override;
    void setAccumulators(int64_t ns, RowData* acc) override;
    RowData* getAccumulators() override;
    RowData* createAccumulators() override;
    RowData* getValue(int64_t ns) override;
    void cleanup(int64_t ns) override;
    void close() override;

private:
    std::vector<int32_t> aggValueTypeIds_;
    std::vector<int32_t> outputValueTypeIds_;
    SliceAssigner* sliceAssigner_{};
    std::vector<std::unique_ptr<NamespaceAggsBasicFunction<int64_t>>> functions_;
    RowData* currentAcc_{};
};
