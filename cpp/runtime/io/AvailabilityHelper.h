/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_AVAILABILITYHELPER_H
#define OMNISTREAM_AVAILABILITYHELPER_H

#include <memory>
#include <string>
#include "AvailabilityProvider.h"

namespace omnistream {

    class AvailabilityHelper : public AvailabilityProvider {
    public:
        AvailabilityHelper();
        ~AvailabilityHelper() override;

        std::shared_ptr<CompletableFuture> and_(const std::shared_ptr<CompletableFuture>& other);
        std::shared_ptr<CompletableFuture> and_(const std::shared_ptr<AvailabilityProvider>& other);
        std::shared_ptr<CompletableFuture> or_(const std::shared_ptr<CompletableFuture>& other);
        std::shared_ptr<CompletableFuture> or_(const std::shared_ptr<AvailabilityProvider>& other);

        void resetUnavailable();
        void resetAvailable();
        std::shared_ptr<CompletableFuture> getUnavailableToResetAvailable();
        std::shared_ptr<CompletableFuture> getUnavailableToResetUnavailable();
        std::shared_ptr<CompletableFuture> getAvailableFuture()  override;
        std::string toString()  override;


    private:
        std::shared_ptr<CompletableFuture> availableFuture;
    };

} // namespace omnistream

#endif // OMNISTREAM_AVAILABILITYHELPER_H
