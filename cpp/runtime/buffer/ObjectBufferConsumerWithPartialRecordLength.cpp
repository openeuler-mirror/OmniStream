/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ObjectBufferConsumerWithPartialRecordLength.h"


#include <sstream>
#include <stdexcept>

namespace omnistream {

    ObjectBufferConsumerWithPartialRecordLength::ObjectBufferConsumerWithPartialRecordLength(std::shared_ptr<ObjectBufferConsumer> bufferConsumer, int partialRecordLength)
        : bufferConsumer(bufferConsumer), partialRecordLength(partialRecordLength) {
        if (!bufferConsumer) {
            throw std::invalid_argument("bufferConsumer cannot be null");
        }
    }

    ObjectBufferConsumerWithPartialRecordLength::~ObjectBufferConsumerWithPartialRecordLength()
    {
        LOG_TRACE("destructor")
    }

    std::shared_ptr<ObjectBufferConsumer> ObjectBufferConsumerWithPartialRecordLength::getBufferConsumer() {
        return bufferConsumer;
    }

    int ObjectBufferConsumerWithPartialRecordLength::getPartialRecordLength() {
        return partialRecordLength;
    }

    std::shared_ptr<VectorBatchBuffer> ObjectBufferConsumerWithPartialRecordLength::build() {
        return bufferConsumer->build();
    }

    bool ObjectBufferConsumerWithPartialRecordLength::cleanupPartialRecord() {
        if (partialRecordLength < 0) {
            throw std::runtime_error("Approximate local recovery does not yet work with unaligned checkpoint!");
        }

        if (partialRecordLength == 0 || !bufferConsumer->isStartOfDataBuffer()) {
            return true;
        }

        if (partialRecordLength > bufferConsumer->getBufferSize()) {
            throw std::runtime_error("Partial record length beyond max buffer capacity!");
        }

        bufferConsumer->skip(partialRecordLength);

        if (partialRecordLength < bufferConsumer->getBufferSize()) {
            return true;
        } else {
            return false;
        }
    }

    std::string ObjectBufferConsumerWithPartialRecordLength::toString() const {
        std::stringstream ss;
        ss << "ObjectBufferConsumerWithPartialRecordLength{";
        ss << "bufferConsumer=" << (bufferConsumer ? "present" : "null") << ", ";
        ss << "partialRecordLength=" << partialRecordLength;
        ss << "}";
        return ss.str();
    }

} // namespace omnistream