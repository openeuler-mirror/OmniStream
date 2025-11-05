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

#ifndef OBJECTSEGMENT_H
#define OBJECTSEGMENT_H

#include <cstddef>
#include <utility>
#include <streaming/runtime/streamrecord/StreamElement.h>
#include <vector/vector.h>
#include "table/data/vectorbatch/VectorBatch.h"
#include "core/memory/Segment.h"

namespace omnistream {
    class ObjectSegment : public Segment {
    public:
        explicit ObjectSegment(size_t size): Segment(SegmentType::OBJECT_SEGMENT), size(size)
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

        size_t getSize()
        {
            return size;
        }
    private:
        size_t size;

        //  it is actually a  StreamRecord * [size] , allocate mem in constructor, StreamRecord.value are VectorBatch *
        //  notice in order to get high performance, the data related object are using raw pointer
        StreamElement** objects_;
    };
}


#endif
