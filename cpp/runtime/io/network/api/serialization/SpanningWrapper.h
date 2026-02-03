/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * @Description: Spanning Wrapper for DataStream
 */
#ifndef FLINK_TNEL_SPANNINGWRAPPER_H
#define FLINK_TNEL_SPANNINGWRAPPER_H

#include <vector>
#include <memory>
#include "core/utils/ByteBuffer.h"
#include "core/memory/DataInputDeserializer.h"
#include "NonSpanningWrapper.h"
namespace omnistream::datastream {
class SpanningWrapper {

public:
    ByteBuffer* lengthBuffer_;

    SpanningWrapper();
    ~SpanningWrapper();
    bool hasFullRecord() const;
    int getNumGatheredBytes() const;
    void clear();

    DataInputView& getInputView();

    void transferLeftOverTo(NonSpanningWrapper& nonSpanningWrapper);
    void transferFrom(NonSpanningWrapper &partial, int nextRecordLength);
    void addNextChunkFromMemoryBuffer(const uint8_t* buffer, int numBytes);
    std::shared_ptr<Buffer> GetUnconsumedSegment()
    {
        LOG("SpanningWrapper GetUnconsumedSegment position: " << lengthBuffer_->position());
        if (lengthBuffer_->position() <= 0) {
            return nullptr;
        }
        uint8_t *data = reinterpret_cast<uint8_t *>(malloc(lengthBuffer_->position()));
        std::shared_ptr<MemorySegment> memorySegment = std::make_shared<MemorySegment>(data, lengthBuffer_->position());
        memorySegment->put(0, lengthBuffer_->getValue(), 0, lengthBuffer_->position());
        std::shared_ptr<::datastream::NetworkBuffer> networkBuffer = std::make_shared<::datastream::NetworkBuffer>(
            memorySegment, lengthBuffer_->position(), 0, std::make_shared<OriginalNetworkBufferRecycler>(),
            ObjectBufferDataType::DATA_BUFFER);
        return networkBuffer;
    }
private:
    std::vector<uint8_t> buffer_; // internal buff to stick data, has the ownership

    int recordLength_;
    int accumulatedRecordBytes_;

    DataInputDeserializer* serializationReadBuffer_;

    const uint8_t* leftOverData_;  // ref to input buffer_ (flink MemSegment), no ownership
    int leftOverStart_;
    int leftOverLimit_;

    bool isReadingLength() const;
    void updateLength(int length);
    int readLength(const uint8_t* buffer, int remaining);
    void ensureBufferCapacity(int minLength);
    void copyIntoBuffer(const uint8_t* buffer,  int offset, int length);
};
}

#endif
