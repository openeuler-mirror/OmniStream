/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once
#include <type_traits>
#include "InternalWindowProcessFunction.h"
#include "MergingWindowSet.h"
#include "../api/common/state/MapStateDescriptor.h"
#include "table/runtime/operators/window/WindowOperator.h"

template<typename K, typename W>
class MergingWindowProcessFunction : public InternalWindowProcessFunction<K, W> {
    static_assert(std::is_base_of_v<Window, W>, "W must inherit from Window");

public:
    MergingWindowProcessFunction(MergingWindowAssigner<W> *windowAssigner,
        NamespaceAggsHandleFunctionBase<W> *windowAggregator,
        TypeSerializer *windowSerializer,
        long allowedLateness): InternalWindowProcessFunction<K, W>(
        std::shared_ptr<MergingWindowAssigner<W>>(windowAssigner),
        std::shared_ptr<NamespaceAggsHandleFunctionBase<W>>(windowAggregator),
        allowedLateness), windowAssigner(windowAssigner) {}

    void Open(Context<K, W> *ctx) override;

    std::vector<W> AssignStateNamespace(RowData *keyRowData, long timestamp) override;

    std::vector<W> AssignActualWindows(RowData *inputRow, long timestamp) override;

    void PrepareAggregateAccumulatorForEmit(W &window) override;

    void CleanWindowIfNeeded(W &window, long currentTime) override;

private:
    MergingWindowAssigner<W> *windowAssigner;
    MergingWindowSet<W> mergingWindows;
    TypeSerializer *windowSerializer;
    std::vector<W> reuseActualWindows;
};

template class MergingWindowProcessFunction<RowData *, TimeWindow>;
