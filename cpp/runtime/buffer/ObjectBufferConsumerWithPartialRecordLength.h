/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OBJECTBUFFERCONSUMERWITHPARTIALRECORDLENGTH_H
#define OMNISTREAM_OBJECTBUFFERCONSUMERWITHPARTIALRECORDLENGTH_H

#include <memory>
#include <string>

#include "ObjectBuffer.h"
#include "ObjectBufferConsumer.h"
#include "VectorBatchBuffer.h"

namespace omnistream {

    class ObjectBufferConsumerWithPartialRecordLength {
    public:
        ObjectBufferConsumerWithPartialRecordLength(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength);
        ~ObjectBufferConsumerWithPartialRecordLength();

        std::shared_ptr<ObjectBufferConsumer> getBufferConsumer();
        int getPartialRecordLength();
        std::shared_ptr<VectorBatchBuffer> build();
        bool cleanupPartialRecord();
        std::string toString() const;

    private:
        std::shared_ptr<ObjectBufferConsumer> bufferConsumer;
        int partialRecordLength;
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERCONSUMERWITHPARTIALRECORDLENGTH_H
