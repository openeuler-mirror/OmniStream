/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#ifndef OMNISTREAM_OBJECTBUFFERPOOLFACTORY_H
#define OMNISTREAM_OBJECTBUFFERPOOLFACTORY_H

#include <memory>
#include <string>
#include <sstream>
#include <iostream>
#include "ObjectBufferPool.h"

namespace omnistream {

    class ObjectBufferPoolFactory {
    public:
        virtual ~ObjectBufferPoolFactory() = default;

        virtual std::shared_ptr<ObjectBufferPool> createBufferPool(int numRequiredBuffers, int maxUsedBuffers) = 0;

        virtual std::shared_ptr<ObjectBufferPool> createBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel) = 0;

        virtual void destroyBufferPool(std::shared_ptr<ObjectBufferPool> bufferPool) = 0;

        virtual std::string toString() const {
            std::stringstream ss;
            ss << "ObjectBufferPoolFactory";
            return ss.str();
        }
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERPOOLFACTORY_H