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

#include "AvailabilityHelper.h"
#include <sstream>

namespace omnistream {


    AvailabilityHelper::AvailabilityHelper() : availableFuture(std::make_shared<CompletableFuture>()) {}

    AvailabilityHelper::~AvailabilityHelper() {}

    std::shared_ptr<CompletableFuture> AvailabilityHelper::and_(const std::shared_ptr<CompletableFuture>& other)
    {
        return AvailabilityProvider::and_(availableFuture, other);
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::and_(const std::shared_ptr<AvailabilityProvider>& other)
    {
        return and_(other->GetAvailableFuture());
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::or_(const std::shared_ptr<CompletableFuture>& other)
    {
        return AvailabilityProvider::or_(availableFuture, other);
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::or_(const std::shared_ptr<AvailabilityProvider>& other)
    {
        return or_(other->GetAvailableFuture());
    }

    void AvailabilityHelper::resetUnavailable()
    {
        if (availableFuture == AVAILABLE) {
            availableFuture = std::make_shared<CompletableFuture>();
        }
    }

    void AvailabilityHelper::resetAvailable()
    {
        availableFuture = AVAILABLE;
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::getUnavailableToResetAvailable()
    {
        std::shared_ptr<CompletableFuture> toNotify = availableFuture;
        availableFuture = AVAILABLE;
        return toNotify;
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::getUnavailableToResetUnavailable()
    {
        std::shared_ptr<CompletableFuture> toNotify = availableFuture;
        availableFuture = std::make_shared<CompletableFuture>();
        return toNotify;
    }

    std::shared_ptr<CompletableFuture> AvailabilityHelper::GetAvailableFuture()
    {
        return availableFuture;
    }

    std::string AvailabilityHelper::toString()
    {
        if (availableFuture == AVAILABLE) {
            return "AVAILABLE";
        }
        return availableFuture->toString();
    }

} // namespace omnistream