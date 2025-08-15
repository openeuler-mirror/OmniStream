/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#ifndef OBJECTSEGMENT_H
#define OBJECTSEGMENT_H

#include <cstddef>
#include <utility>
#include <include/functions/StreamElement.h>
#include <vector/vector.h>
#include "vectorbatch/VectorBatch.h"

namespace omnistream
{
    class ObjectSegment
    {
    public:
        explicit ObjectSegment(size_t size): size(size)
        {
            objects_ = new StreamElement* [size];
        }

        ~ObjectSegment()
        {
            delete[] objects_;
        }

        int putObject(int offset, StreamElement* record)
        {
            LOG("objects address" << objects_[offset])
            LOG("objects size()" << size)
            objects_[offset] = record;
            return 1; // written size
        }

        StreamElement* getObject(int offset)
        {
            return objects_[offset];
        }

        size_t getSize() {
            return size;
        }
    private:
        size_t size;

        //  it is actually a  StreamRecord * [size] , allocate mem in constructor, StreamRecord.value are VectorBatch *
        //  notice in order to get high performance, the data related object are using raw pointer
        StreamElement** objects_;
    };
}


#endif  //OBJECTSEGMENT_H
