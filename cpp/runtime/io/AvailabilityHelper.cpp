/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "AvailabilityHelper.h"
#include <sstream>

namespace omnistream {


    AvailabilityHelper::AvailabilityHelper() : availableFuture(std::make_shared<CompletableFuture>()) {}

    AvailabilityHelper::~AvailabilityHelper() {}

    std::shared_ptr<CompletableFuture> AvailabilityHelper::and_(const std::shared_ptr<CompletableFuture>& other) {
        return AvailabilityProvider::and_(availableFuture, other);
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::and_(const std::shared_ptr<AvailabilityProvider>& other) {
        return and_(other->getAvailableFuture());
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::or_(const std::shared_ptr<CompletableFuture>& other) {
        return AvailabilityProvider::or_(availableFuture, other);
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::or_(const std::shared_ptr<AvailabilityProvider>& other) {
        return or_(other->getAvailableFuture());
    }

    void AvailabilityHelper::resetUnavailable() {
        if (availableFuture == AVAILABLE) {
            availableFuture = std::make_shared<CompletableFuture>();
        }
    }

    void AvailabilityHelper::resetAvailable() {
        availableFuture = AVAILABLE;
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::getUnavailableToResetAvailable() {
        std::shared_ptr<CompletableFuture> toNotify = availableFuture;
        availableFuture = AVAILABLE;
        return toNotify;
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::getUnavailableToResetUnavailable() {
        std::shared_ptr<CompletableFuture> toNotify = availableFuture;
        availableFuture = std::make_shared<CompletableFuture>();
        return toNotify;
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::getAvailableFuture()  {
        return availableFuture;
    }

    std::string AvailabilityHelper::toString()  {
        if (availableFuture == AVAILABLE) {
            return "AVAILABLE";
        }
        return availableFuture->toString();
    }

} // namespace omnistream