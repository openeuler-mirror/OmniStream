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

        virtual std::string toString() const
        {
            std::stringstream ss;
            ss << "ObjectBufferPoolFactory";
            return ss.str();
        }
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERPOOLFACTORY_H