/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef STREAMRECORDHELPER_H
#define STREAMRECORDHELPER_H

#include <vectorbatch/VectorBatch.h>
#include "StreamRecord.h"
class StreamRecordHelper {
public:
    static  StreamRecord* deepCopyVectorBatch(StreamRecord* origin)
    {
        auto record = origin;
        long timestamp = record->getTimestamp();
        auto value = record->getValue();
        auto vectorBatch = reinterpret_cast<omnistream::VectorBatch*>(value);
        auto copiedVectorBatch = vectorBatch->copy();
        return new StreamRecord(copiedVectorBatch, timestamp);
    }

    static  StreamRecord* deepCopyObject(StreamRecord* origin)
    {
        auto record = origin;
        long timestamp = record->getTimestamp();
        auto value = record->getValue();
        auto object = reinterpret_cast<Object*>(value);
        auto copiedObject = object->clone();
        return new StreamRecord(copiedObject, timestamp);
    }
};


#endif //STREAMRECORDHELPER_H
