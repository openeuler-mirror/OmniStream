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

#ifndef MEMORYBUFFERBUIDLER_H
#define MEMORYBUFFERBUIDLER_H

#include <memory>
#include <string>

#include "core/memory/MemorySegment.h"
#include "NetworkBuffer.h"
#include "BufferBuilder.h"
#include "MemoryBufferConsumer.h"

namespace datastream {


class MemoryBufferBuilder : public BufferBuilder {
public:
    MemoryBufferBuilder(std::shared_ptr<MemorySegment> memorySegment, std::shared_ptr<BufferRecycler> recycler);
    ~MemoryBufferBuilder() override = default;

    int append(void *source) override;

    using BufferBuilder::createBufferConsumer;
    std::shared_ptr<BufferConsumer> createBufferConsumerFromBeginning() override;
    std::shared_ptr<BufferConsumer> createBufferConsumer(int currentReaderPosition) override;

    std::string toString() override;

private:
    std::shared_ptr<MemorySegment> memorySegment;
    bool bufferConsumerCreated;
};

}


#endif // MEMORYBUFFERBUIDLER_H
