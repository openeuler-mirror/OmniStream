/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/1/25.
//

#ifndef OMNISTREAM_OMNISTREAMINPUTPROCESSOR_H
#define OMNISTREAM_OMNISTREAMINPUTPROCESSOR_H

#include "AvailabilityProvider.h"
#include "io/DataInputStatus.h"

namespace omnistream {

    class OmniStreamInputProcessor : public AvailabilityProvider {
    public:
        virtual DataInputStatus processInput() = 0;

        std::shared_ptr<CompletableFuture> getAvailableFuture() override {
            return nullptr;
        }

        virtual void close() {}
    };
}


#endif //OMNISTREAM_OMNISTREAMINPUTPROCESSOR_H
