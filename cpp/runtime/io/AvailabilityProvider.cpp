/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "AvailabilityProvider.h"
#include <sstream>

namespace omnistream {

    std::shared_ptr<CompletableFuture> AvailabilityProvider::AVAILABLE = std::make_shared<CompletableFuture>();

    std::shared_ptr<CompletableFuture> AvailabilityProvider::and_(std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second) {
        if (first == AVAILABLE && second == AVAILABLE) {
            return AVAILABLE;
        } else if (first == AVAILABLE) {
            return second;
        } else if (second == AVAILABLE) {
            return first;
        } else {
            return CompletableFuture::allOf(std::vector<std::shared_ptr<CompletableFuture>>{first, second});
        }
    }

    std::shared_ptr<CompletableFuture> AvailabilityProvider::or_(std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second) {
        if (first == AVAILABLE || second == AVAILABLE) {
            return AVAILABLE;
        }
        return CompletableFuture::anyOf(std::vector<std::shared_ptr<CompletableFuture>>{first, second});
    }


} // namespace omnistream
