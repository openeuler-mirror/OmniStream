/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef INTERNALWINDOWPROCESSFUNCTION_H
#define INTERNALWINDOWPROCESSFUNCTION_H

#pragma once

#include <memory>
#include <vector>

#include "table/runtime/operators/window/assigners/WindowAssigner.h"
#include "table/runtime/operators/window/Window.h"
#include "table/runtime/generated/NamespaceAggsHandleFunctionBase.h"
#include "table/data/RowData.h"

template<typename K, typename W>
class Context {
public:
    virtual ~Context() = default;

    virtual K CurrentKey() = 0;

    virtual long CurrentProcessingTime() = 0;

    virtual long CurrentWatermark() = 0;

    virtual RowData *GetWindowAccumulators(W &window) = 0;

    virtual void SetWindowAccumulators(W &window, RowData *accumulators) = 0;

    virtual void ClearWindowState(W &window) = 0;

    virtual void ClearPreviousState(W window) = 0;

    virtual void ClearTrigger(W &window) = 0;

    virtual void OnMerge(W &window, std::vector<W> &windows) = 0;

    virtual void DeleteCleanupTimer(W &window) = 0;
};

template<typename K, typename W>
class InternalWindowProcessFunction {
    static_assert(std::is_base_of_v<Window, W>, "typename W must inherit from Window");

public:
    using AssignerPtr = std::shared_ptr<WindowAssigner<W>>;
    using FunctionBasePtr = std::shared_ptr<NamespaceAggsHandleFunctionBase<W>>;

    InternalWindowProcessFunction(AssignerPtr windowAssigner, FunctionBasePtr windowAggregator,
        long allowedLateness): windowAssigner(windowAssigner), windowAggregator(windowAggregator),
        allowedLateness(allowedLateness) {}

    virtual ~InternalWindowProcessFunction() = default;

    virtual std::vector<W> AssignStateNamespace(RowData *inputRow, long timestamp) = 0;

    virtual std::vector<W> AssignActualWindows(RowData *inputRow, long timestamp) = 0;

    virtual void PrepareAggregateAccumulatorForEmit(W &window) = 0;

    virtual void CleanWindowIfNeeded(W &window, long currentTime) = 0;

    virtual void Close() {}

    virtual void Open(Context<K, W> *ctx_)
    {
        this->ctx = std::unique_ptr<Context<K, W>>(ctx_);
        windowAssigner->Open();
    }

protected:
    std::shared_ptr<WindowAssigner<W>> windowAssigner;
    std::shared_ptr<NamespaceAggsHandleFunctionBase<W>> windowAggregator;
    long allowedLateness;
    std::shared_ptr<Context<K, W>> ctx;

    bool IsCleanupTime(W &window, long time) const
    {
        return time == CleanupTime(window);
    }

    bool IsWindowLate(W &window) const
    {
        return windowAssigner->IsEventTime() && CleanupTime(window) <= ctx->CurrentWatermark();
    }

private:
    long CleanupTime(W &window) const
    {
        if (windowAssigner->IsEventTime()) {
            long cleanupTime = window.maxTimestamp() + allowedLateness;
            return cleanupTime >= window.maxTimestamp() ? cleanupTime : std::numeric_limits<long>::max();
        }
        return window.maxTimestamp();
    }
};

#endif
