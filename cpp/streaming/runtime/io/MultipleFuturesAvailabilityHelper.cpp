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

#include "MultipleFuturesAvailabilityHelper.h"

namespace omnistream {

    void MultipleFuturesAvailabilityHelper::notifyCompletion() {
        availableFuture->complete();
    }

    MultipleFuturesAvailabilityHelper::MultipleFuturesAvailabilityHelper(int size)
    {
        futuresToCombine.resize(size);
    }

    std::shared_ptr<CompletableFuture> MultipleFuturesAvailabilityHelper::getAvailableFuture()
    {
        return availableFuture;
    }

    void MultipleFuturesAvailabilityHelper::resetToUnAvailable()
    {
        if (availableFuture->isDone()) {
            availableFuture = std::make_shared<CompletableFuture>();
        }
    }

    void MultipleFuturesAvailabilityHelper::anyOf(int idx, const std::shared_ptr<CompletableFuture>& availabilityFuture)
    {
        if (futuresToCombine[idx] == nullptr || futuresToCombine[idx]->isDone()) {
            futuresToCombine[idx] = availabilityFuture;
            auto inner = std::make_shared<InnerRunnable>(this);
            availabilityFuture->thenRun(inner);
        }
    }

}