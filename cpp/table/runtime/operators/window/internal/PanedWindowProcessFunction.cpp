/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
 * See the Mulan PSL v2 for more details.
 */

#include "PanedWindowProcessFunction.h"

#include <memory>

#include "table/runtime/operators/window/TimeWindow.h"

template<typename K, typename W>
std::vector<W> PanedWindowProcessFunction<K, W>::assignActualWindows(RowData *inputRow, int64_t timestamp) {
    std::vector<W> elementWindows = windowAssigner->assignWindows(inputRow, timestamp);
    std::vector<W> actualWindows;
    actualWindows.reserve(elementWindows.size());
    for (const auto& window : elementWindows) {
        if (!this->isWindowLate(window)) {
            actualWindows.push_back(window);
        }
    }
    return actualWindows;
}

template<typename K, typename W>
std::vector<W> PanedWindowProcessFunction<K, W>::assignStateNamespace(RowData *inputRow, int64_t timestamp) {
    W pane = windowAssigner->assignPane(inputRow, timestamp);
    if (isPaneLate(pane)) {
        return {};
    }
    return {pane};
}

template<typename K, typename W>
void PanedWindowProcessFunction<K, W>::prepareAggregateAccumulatorForEmit(const W& window) {
    std::vector<W> panes = windowAssigner->splitIntoPanes(window);
    RowData *acc = this->windowAggregator->createAccumulators(accumulatorArity_);
    this->windowAggregator->setAccumulators(window, acc);
    for (const auto& pane : panes) {
        RowData *paneAcc = this->ctx->getWindowAccumulators(pane);
        if (paneAcc != nullptr) {
            this->windowAggregator->merge(pane, paneAcc);
        }
    }
}

template<typename K, typename W>
void PanedWindowProcessFunction<K, W>::cleanWindowIfNeeded(const W& window, int64_t currentTime) {
    if (this->isCleanupTime(window, currentTime)) {
        std::vector<W> panes = windowAssigner->splitIntoPanes(window);
        for (const auto& pane : panes) {
            W lastWindow = windowAssigner->getLastWindow(pane);
            if (window == lastWindow) {
                this->ctx->clearWindowState(pane);
            }
        }
        this->ctx->clearTrigger(window);
        this->ctx->clearPreviousState(window);
    }
}

template<typename K, typename W>
bool PanedWindowProcessFunction<K, W>::isPaneLate(const W& pane) {
    return windowAssigner->isEventTime() && this->isWindowLate(windowAssigner->getLastWindow(pane));
}

template class PanedWindowProcessFunction<std::shared_ptr<RowData>, TimeWindow>;
