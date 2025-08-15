/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Spanning Wrapper for DataStream
 */
#include <cstring>
#include "../utils/utils.h"
#include "../include/common.h"
#include "SpanningWrapper.h"
namespace omnistream::datastream {
SpanningWrapper::SpanningWrapper() : buffer_(1024)
{
    lengthBuffer_ = new ByteBuffer(LENGTH_BYTES);
    serializationReadBuffer_ = new DataInputDeserializer();

    recordLength_ = -1;
    accumulatedRecordBytes_ = 0;

    leftOverData_ = nullptr;
    leftOverStart_ = 0 ;
    leftOverLimit_ = 0;
}

SpanningWrapper::~SpanningWrapper()
{
    delete lengthBuffer_;
    delete serializationReadBuffer_;
}

bool SpanningWrapper::hasFullRecord() const
{
    bool result = recordLength_ >= 0 && accumulatedRecordBytes_ >= recordLength_;
    // LOG("hasFullRecord is " << result)
#ifdef DEBUG
    if (result)
        PRINT_HEX(const_cast<uint8_t*>(buffer_.data()), 0, recordLength_);
#endif
    return  result;
}

DataInputView &SpanningWrapper::getInputView()
{
    return *serializationReadBuffer_;
}

void SpanningWrapper::transferLeftOverTo(NonSpanningWrapper &nonSpanningWrapper)
{
    nonSpanningWrapper.clear();

    if (leftOverData_ != nullptr) {
        nonSpanningWrapper.initializeFromMemoryBuffer(
            leftOverData_ + leftOverStart_, leftOverLimit_ - leftOverStart_);
    }
    clear();
}

void SpanningWrapper::clear()
{
    recordLength_ = -1;
    accumulatedRecordBytes_ = 0;

    lengthBuffer_->clear();

    leftOverData_ = nullptr;
    leftOverStart_ = 0;
    leftOverLimit_ = 0;
}

// the function will be called after nonspanningwtrapper has read all full record. now the left is not full record length
// copy all nonspaccingwrapper remainings to spaandingwtriper buff
void SpanningWrapper::transferFrom(NonSpanningWrapper &partial, int nextRecordLength)
{
    // LOG("nextRecordLength :" << nextRecordLength)
    updateLength(nextRecordLength);
    accumulatedRecordBytes_ =
             partial.copyContentTo(buffer_.data());
    // LOG("after partial.copyContentTo : accumulatedRecordBytes_ is " << accumulatedRecordBytes_)
    partial.clear();
}

// numBytes total number of bytes of buffer
void SpanningWrapper::addNextChunkFromMemoryBuffer(const uint8_t *buffer, int numBytes)
{
    //1. if the record length is not completed, get it
    // LOG("isReadingLength()  " << isReadingLength())
#ifdef  DEBUG
    ByteBuffer::showInternalInfo(lengthBuffer_);
#endif
    int numBytesRead = isReadingLength() ? readLength(buffer, numBytes) : 0;
    int offset = numBytesRead;
    int remainNumBytes = numBytes - numBytesRead;
    // LOG("numBytesRead ()  " << numBytesRead  << " numBytes   " << numBytes  <<   " remainNumBytes  " << remainNumBytes  )
    if (remainNumBytes == 0) {
        return;
    }

    //2. now the record length is ready, get the whole record
    // LOG("recordLength_  " << recordLength_   << " accumulatedRecordBytes_    " << accumulatedRecordBytes_  )
    int toCopy = std::min(recordLength_ - accumulatedRecordBytes_, remainNumBytes);
    // LOG("offset  " << offset   << " toCopy    " << toCopy  )
    if (toCopy > 0) {
        copyIntoBuffer(buffer, offset, toCopy);
    }

    //3. if there is leftover data in buffer_, ref the left one through leftover
    if (remainNumBytes > toCopy) {
        leftOverData_ = buffer;
        leftOverStart_ = offset + toCopy;
        leftOverLimit_ = numBytes;
    }
}


// private member functions
bool SpanningWrapper::isReadingLength() const
{
    return lengthBuffer_->position() > 0;
}

void SpanningWrapper::updateLength(int length)
{
    lengthBuffer_->clear();
    recordLength_ = length;
    ensureBufferCapacity(length);
}

void SpanningWrapper::ensureBufferCapacity(int minLength)
{
    if (static_cast<size_t>(minLength) > buffer_.capacity()) {
        int newCapacity_ = std::max(minLength, static_cast<int>(buffer_.capacity() * 2));
        buffer_.reserve(newCapacity_);
    }
}

int SpanningWrapper::readLength(const uint8_t *buffer, int remaining)
{
    // LOG("lengthBuffer_->remaining(): "  << lengthBuffer_->remaining()  << " remaining:  "<< remaining)
    int bytesToRead = std::min(lengthBuffer_->remaining(), remaining);
    // LOG(" bytesToRead:  "<< bytesToRead)
    lengthBuffer_->putBytes(buffer, bytesToRead);
    // LOG("After put bytes lengthBuffer_->remaining(): "  << lengthBuffer_->remaining())
    if (!lengthBuffer_->hasRemaining()) {
        updateLength(lengthBuffer_->getIntBigEndian(0));
    }
    return bytesToRead;
}

void SpanningWrapper::copyIntoBuffer(const uint8_t *buffer, int offset, int length)
{
    auto ret = memcpy_s(buffer_.data() + accumulatedRecordBytes_, length, buffer + offset, length);
    if (ret != EOK) {
        throw std::runtime_error("memcpy_s failed");
    }
    accumulatedRecordBytes_ += length;
    if (hasFullRecord()) {
        // LOG("buffer_" << reinterpret_cast <long> (buffer_.data()) << " buffer size " << buffer_.size())
        // LOG("accumulatedRecordBytes_ " << accumulatedRecordBytes_  << " recordLength_ " << recordLength_)
        serializationReadBuffer_->setBuffer(buffer_.data(), accumulatedRecordBytes_, 0, recordLength_);
    }
}

int SpanningWrapper::getNumGatheredBytes() const
{
    return accumulatedRecordBytes_
           + (recordLength_ >= 0 ? LENGTH_BYTES : lengthBuffer_->position());
}
}