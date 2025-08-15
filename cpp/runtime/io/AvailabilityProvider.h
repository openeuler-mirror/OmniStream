/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//


#ifndef OMNISTREAM_AVAILABILITY_PROVIDER_H
#define OMNISTREAM_AVAILABILITY_PROVIDER_H

#include <memory>
#include <sstream>
#include "core/utils/threads/CompletableFuture.h"

namespace omnistream {

    class AvailabilityProvider {
    public:
        AvailabilityProvider() {}
        virtual ~AvailabilityProvider() {}

        static std::shared_ptr<CompletableFuture> AVAILABLE;

        virtual  std::shared_ptr<CompletableFuture> getAvailableFuture()  =0;

        static std::shared_ptr<CompletableFuture> and_(std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second);
        static std::shared_ptr<CompletableFuture> or_(std::shared_ptr<CompletableFuture> first, std::shared_ptr<CompletableFuture> second);

        bool isAvailable()
        {
            auto future = getAvailableFuture();
            return future && (future == AVAILABLE || future->isDone());
        }

        bool isApproximatelyAvailable()
        {
            return getAvailableFuture() == AVAILABLE;
        }

        virtual std::string toString()
        {
            std::stringstream ss;
            ss << "AvailabilityProvider: ";
            if (isAvailable()) {
                ss << "Available";
            } else {
                ss << "Not Available";
            }
            return ss.str();
        }
    };


} // namespace omnistream

#endif // OMNISTREAM_AVAILABILITY_PROVIDER_H