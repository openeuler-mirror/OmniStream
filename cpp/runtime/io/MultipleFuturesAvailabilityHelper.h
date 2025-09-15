/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_MULTIPLEFUTURESAVAILABILITYHELPER_H
#define OMNISTREAM_MULTIPLEFUTURESAVAILABILITYHELPER_H

#include "core/utils/threads/CompletableFuture.h"

namespace omnistream {
    class MultipleFuturesAvailabilityHelper {
    private:
        std::vector<std::shared_ptr<CompletableFuture>> futuresToCombine;
        std::shared_ptr<CompletableFuture> availableFuture = std::make_shared<CompletableFuture>();

        void notifyCompletion();
    public:
        class InnerRunnable : public Runnable {
        private:
            MultipleFuturesAvailabilityHelper* outer;
        public:
            explicit InnerRunnable(MultipleFuturesAvailabilityHelper* outer) : outer(outer) {}

            void run() override
            {
                outer->notifyCompletion();
            }
        };

        explicit MultipleFuturesAvailabilityHelper(int size);

        std::shared_ptr<CompletableFuture> getAvailableFuture();

        void resetToUnAvailable();

        void anyOf(int idx, std::shared_ptr<CompletableFuture> availabilityFuture);
    };
}


#endif //OMNISTREAM_MULTIPLEFUTURESAVAILABILITYHELPER_H
