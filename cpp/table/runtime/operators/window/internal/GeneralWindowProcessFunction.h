/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

#pragma once

#include <cstdint>
#include <type_traits>
#include <vector>

#include "InternalWindowProcessFunction.h"

template<typename K, typename W>
class GeneralWindowProcessFunction : public InternalWindowProcessFunction<K, W> {
    static_assert(std::is_base_of_v<Window, W>, "W must inherit from Window");

public:
    GeneralWindowProcessFunction(
            WindowAssigner<W> *windowAssigner,
            NamespaceAggsHandleFunctionBase<W> *windowAggregator,
            int64_t allowedLateness,
            int32_t accumulatorArity)
            :
            InternalWindowProcessFunction<K, W>(windowAssigner, windowAggregator, allowedLateness),
            windowAssigner(windowAssigner),
            accumulatorArity_(accumulatorArity) {}

    std::vector<W> assignStateNamespace(RowData *inputRow, int64_t timestamp) override;

    std::vector<W> assignActualWindows(RowData *inputRow, int64_t timestamp) override;

    void prepareAggregateAccumulatorForEmit(const W& window) override;

    void cleanWindowIfNeeded(const W& window, int64_t currentTime) override;

private:
    WindowAssigner<W>* windowAssigner{};
    std::vector<W> reuseAffectedWindows;
    int32_t accumulatorArity_ = -1;
};
