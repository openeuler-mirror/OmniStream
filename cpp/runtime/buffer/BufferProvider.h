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

#ifndef BUFFERPROVIDER_H
#define BUFFERPROVIDER_H

#include "BufferBuilder.h"
#include "BufferListener.h"
#include "Buffer.h"
#include <memory>
#include <string>
#include <io/AvailabilityProvider.h>


namespace omnistream {
    class BufferProvider : public AvailabilityProvider   {
    public:
        ~BufferProvider() override = default;

        virtual std::shared_ptr<Buffer> requestBuffer() = 0;

        virtual BufferBuilder *requestBufferBuilder() = 0;

        virtual BufferBuilder *requestBufferBuilder(int targetChannel) = 0;

        virtual BufferBuilder *requestBufferBuilderBlocking() = 0;

        virtual BufferBuilder *requestBufferBuilderBlocking(int targetChannel) = 0;

        virtual bool addBufferListener(std::shared_ptr<BufferListener> listener) = 0;

        virtual bool isDestroyed() = 0;

        virtual std::string toString() const = 0;
    };
}

#endif // BUFFERPROVIDER_H
