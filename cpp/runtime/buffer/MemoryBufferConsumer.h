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

#ifndef MEMORYBUFFERCONSUMER_H
#define MEMORYBUFFERCONSUMER_H

#include <memory>
#include <string>

#include "NetworkBuffer.h"
#include "PositionMarker.h"
#include "BufferConsumer.h"

using namespace omnistream;

namespace datastream {

    class MemoryBufferConsumer : public BufferConsumer {
    public:
        MemoryBufferConsumer(std::shared_ptr<NetworkBuffer> buffer, int size);
        MemoryBufferConsumer(std::shared_ptr<NetworkBuffer> buffer, std::shared_ptr<PositionMarker> positionMarker, int readerPosition);

        ~MemoryBufferConsumer() override = default;

        std::shared_ptr<Buffer> build() override;
        std::shared_ptr<BufferConsumer> copy() override;
        std::shared_ptr<BufferConsumer> copyWithReaderPosition(int readerPosition) override;

        std::shared_ptr<NetworkBuffer> buildNetworkBuffer();

        bool isStartOfDataBuffer() const override;
        std::string toDebugString(bool includeHash) override;
        std::string toString() override;
    };

}


#endif // MEMORYBUFFERCONSUMER_H
