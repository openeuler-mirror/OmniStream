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

#include "NamespaceAggsBasicFunction.h"

enum class NamespaceAggsBasicFunctionType;

template <typename N>
class NamespaceAggsMinMaxFunction : public NamespaceAggsBasicFunction<N> {
public:
    NamespaceAggsMinMaxFunction(
        std::vector<int32_t> argIndexes,
        std::vector<int32_t> inputTypeIds,
        std::vector<int32_t> accIndexes,
        std::vector<int32_t> accTypeIds,
        int32_t aggValueIndex,
        int32_t aggValueTypeId,
        NamespaceAggsBasicFunctionType type);

    void accumulate(RowData* input) override;
    void retract(RowData* input) override;
    void merge(N ns, RowData* otherAcc) override;
    void setAccumulators(N ns, RowData* acc) override;
    RowData* getAccumulators() override;
    void updateAggValue(RowData* input) override;

private:
    void update(int64_t candidate);

    NamespaceAggsBasicFunctionType type_;
    int64_t value_ = 0L;
};
