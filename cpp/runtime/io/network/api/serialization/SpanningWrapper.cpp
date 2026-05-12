/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * @Description: Spanning Wrapper for DataStream
 */
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
}