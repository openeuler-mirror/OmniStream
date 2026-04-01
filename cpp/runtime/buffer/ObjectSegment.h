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

#include <cstdint>
#include <cstddef>
#include <utility>
#include <streaming/runtime/streamrecord/StreamElement.h>
#include <vector/vector.h>
#include "basictypes/SizeAwareObject.h"
#include "core/memory/Segment.h"

namespace omnistream {
    class ObjectSegment : public Segment, public SizeAwareObject {
    public:
        explicit ObjectSegment(size_t size);

        ~ObjectSegment() override;

        int putObject(int offset, StreamElement* record);

        StreamElement* getObject(int offset);

        [[nodiscard]] size_t getSize() const;

        [[nodiscard]] int64_t getObjectSizeInBytes() const override;
        void reset();
        int64_t getCapacity();
        void setCapacity(int64_t capacity);

        // Byte size one stored StreamElement contributes. Public so a sliced buffer can sum the
        // bytes over its slot range when recycling.
        static int64_t calculateStoredObjectSizeInBytes(const StreamElement* record);
    private:

        size_t size;
        int64_t sizeInBytes_ = 0;
        int64_t capacityInBytes_ = 0;
        //  it is actually a  StreamRecord * [size] , allocate mem in constructor, StreamRecord.value are VectorBatch *
        //  notice in order to get high performance, the data related object are using raw pointer
        StreamElement** objects_;
    };
}


#endif
