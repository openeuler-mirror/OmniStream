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

#ifndef OMNISTREAM_BUFFERBUILDER_H
#define OMNISTREAM_BUFFERBUILDER_H

#include <memory>
#include <string>

#include "BufferBuilder.h"
#include "ObjectBufferConsumer.h"
#include "ObjectSegment.h"

namespace omnistream {

class ObjectBufferBuilder : public BufferBuilder {
public:
    ObjectBufferBuilder(std::shared_ptr<ObjectSegment> objSegment, std::shared_ptr<BufferRecycler> recycler);
    ~ObjectBufferBuilder() override = default;

    int append(void *source) override;

    using BufferBuilder::createBufferConsumer;
    std::shared_ptr<BufferConsumer> createBufferConsumerFromBeginning() override;
    std::shared_ptr<BufferConsumer> createBufferConsumer(int currentReaderPosition) override;

    std::string toString() override ;

    // for test
    StreamElement* getObject(int index);
private:

    std::shared_ptr<ObjectSegment> objSegment;
    bool bufferConsumerCreated;
};

} // namespace omnistream

#endif // OMNISTREAM_BUFFERBUILDER_H