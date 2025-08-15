/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//


#ifndef OMNISTREAM_OBJECTBUFFERPROVIDER_H
#define OMNISTREAM_OBJECTBUFFERPROVIDER_H

#include <memory>
#include <string>
#include <io/AvailabilityProvider.h>


#include "ObjectBuffer.h"
#include "ObjectBufferBuilder.h"
#include "ObjectBufferListener.h"

namespace omnistream {

    class ObjectBufferProvider : public AvailabilityProvider {
    public:
        virtual ~ObjectBufferProvider() = default;

        virtual std::shared_ptr<ObjectBuffer> requestBuffer() = 0;

        virtual std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilder() = 0;

        virtual std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilder(int targetChannel) = 0;

        virtual std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilderBlocking() = 0;

        virtual std::shared_ptr<ObjectBufferBuilder> requestObjectBufferBuilderBlocking(int targetChannel) = 0;

        virtual bool addBufferListener(std::shared_ptr<ObjectBufferListener> listener) = 0;

        virtual bool isDestroyed() = 0;

        virtual std::string toString() const = 0;
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERPROVIDER_H
