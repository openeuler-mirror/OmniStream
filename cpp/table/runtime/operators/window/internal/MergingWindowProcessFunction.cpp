/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "MergingWindowProcessFunction.h"

template<typename K, typename W>
void MergingWindowProcessFunction<K, W>::Open(Context<K, W> *ctx)
{
    InternalWindowProcessFunction<K, W>::Open(ctx);
    auto assigner = std::shared_ptr<MergingWindowAssigner<W>>(windowAssigner);

    // init windowState
    windowSerializer = new TimeWindow::Serializer();
    MapStateDescriptor *mapStateDescriptor = new MapStateDescriptor("session-window-mapping", windowSerializer, windowSerializer);
    MapState<W, W>* mapState = dynamic_cast<typename WindowOperator<K, W>::WindowContext*>(ctx)->GetPartitionedState(mapStateDescriptor);
    mergingWindows = MergingWindowSet<W>(assigner, mapState);
}

template<typename K, typename W>
std::vector<W> MergingWindowProcessFunction<K, W>::AssignStateNamespace(RowData *keyRowData, long timestamp)
{
    std::vector<W> elementWindows = windowAssigner->AssignWindows(keyRowData, timestamp);
    mergingWindows.InitializeCache(*dynamic_cast<BinaryRowData*>(keyRowData));
    reuseActualWindows = std::vector<W>();

    auto MergingFunction = [this](W &mergeResult, std::unordered_set<W, MyKeyHash> &mergedWindows, W &stateWindowResult,
        std::vector<W> &stateWindowsToBeMerged) {
        long mergeResultMaxTs = mergeResult.maxTimestamp();
        if (!windowAssigner->IsEventTime() && mergeResultMaxTs <= this->ctx->CurrentProcessingTime()) {
            throw std::runtime_error(
                "The end timestamp of a processing-time window cannot become earlier than the current processing time ");
        }

        this->ctx->OnMerge(mergeResult, stateWindowsToBeMerged);

        // clear registered timers
        for (W m: mergedWindows) {
            this->ctx->ClearTrigger(m);
            // todo 已实现删除eventTimeQueue元素功能, 待测试
            this->ctx->DeleteCleanupTimer(m);
        }

        // merge the merged state windows into the newly resulting state window
        if (!stateWindowsToBeMerged.empty()) {
            RowData *targetAcc = this->ctx->GetWindowAccumulators(stateWindowResult);
            if (targetAcc == nullptr) {
                targetAcc = this->windowAggregator->createAccumulators(0);
            }
            this->windowAggregator->setAccumulators(stateWindowResult, targetAcc);
            for (W w: stateWindowsToBeMerged) {
                RowData *acc = this->ctx->GetWindowAccumulators(w);
                if (acc != nullptr) {
                    this->windowAggregator->merge(w, acc);
                }
                // clear merged window
                this->ctx->ClearWindowState(w);
                this->ctx->ClearPreviousState(w);
            }
            targetAcc = this->windowAggregator->getAccumulators();
            this->ctx->SetWindowAccumulators(stateWindowResult, targetAcc);
        }
    };

    for (const auto &window: elementWindows) {
        // adding the new window might result in a merge, in that case the actualWindow
        // is the merged window and we work with that. If we don't merge then
        // actualWindow == window
        W actualWindow = mergingWindows.AddWindow(window, MergingFunction);
        // drop if the window is already late, not for now
        if (this->IsWindowLate(actualWindow)) {
            mergingWindows.RetireWindow(actualWindow);
        } else {
            reuseActualWindows.push_back(actualWindow);
        }
    }

    std::vector<W> affectedWindows;
    affectedWindows.reserve(reuseActualWindows.size());
    for (const auto &actual: reuseActualWindows) {
        affectedWindows.push_back(mergingWindows.GetStateWindow(actual));
    }
    return affectedWindows;
}

template<typename K, typename W>
std::vector<W> MergingWindowProcessFunction<K, W>::AssignActualWindows(RowData *inputRow, long timestamp)
{
    return reuseActualWindows;
}

template<typename K, typename W>
void MergingWindowProcessFunction<K, W>::PrepareAggregateAccumulatorForEmit(W &window)
{
    W stateWindow = mergingWindows.GetStateWindow(window);
    RowData *acc = this->ctx->GetWindowAccumulators(stateWindow);
    if (acc == nullptr) {
        acc = InternalWindowProcessFunction<K, W>::windowAggregator->createAccumulators(1);
    }
    InternalWindowProcessFunction<K, W>::windowAggregator->setAccumulators(stateWindow, acc);
}

template<typename K, typename W>
void MergingWindowProcessFunction<K, W>::CleanWindowIfNeeded(W &window, long currentTime)
{
    if (this->IsCleanupTime(window, currentTime)) {
        this->ctx->ClearTrigger(window);
        W stateWindow = mergingWindows.GetStateWindow(window);
        this->ctx->ClearWindowState(stateWindow);
        // retire expired window
        BinaryRowData *currentKey = dynamic_cast<BinaryRowData*>(this->ctx->CurrentKey());
        if (currentKey != nullptr) {
            mergingWindows.InitializeCache(*currentKey);
        } else {
            std::cerr << "Error: CurrentKey is not of type BinaryRowData" << std::endl;
        }
        mergingWindows.RetireWindow(window);
        // do not need to clear previous state, previous state is disabled in session window
    }
}
