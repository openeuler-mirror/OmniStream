/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "MultipleFuturesAvailabilityHelper.h"

namespace omnistream {

    void MultipleFuturesAvailabilityHelper::notifyCompletion() {
    }

    MultipleFuturesAvailabilityHelper::MultipleFuturesAvailabilityHelper(int size) {
        futuresToCombine.reserve(size);
    }

    std::shared_ptr<CompletableFuture> MultipleFuturesAvailabilityHelper::getAvailableFuture() {
        return availableFuture;
    }

    void MultipleFuturesAvailabilityHelper::resetToUnAvailable() {
        if (availableFuture->isDone()) {
            availableFuture = std::make_shared<CompletableFuture>();
        }
    }

    void MultipleFuturesAvailabilityHelper::anyOf(int idx, std::shared_ptr<CompletableFuture> availabilityFuture) {
        if (futuresToCombine[idx] == nullptr || futuresToCombine[idx]->isDone()) {
            futuresToCombine[idx] = availabilityFuture;
            availabilityFuture->thenRun(new InnerRunnable(this));
        }
    }

}