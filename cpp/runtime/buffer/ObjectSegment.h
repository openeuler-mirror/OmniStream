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

#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <utility>
#include "table/data/Row.h"
#include <streaming/runtime/streamrecord/StreamElement.h>
#include <streaming/runtime/streamrecord/StreamRecord.h>
#include <streaming/api/watermark/Watermark.h>
#include <vector/vector.h>

#include "table/data/vectorbatch/VectorBatch.h"
#include "core/memory/Segment.h"

namespace omnistream {
class ObjectSegmentChannelStateSerde;

class ObjectSegment : public Segment {
public:
    explicit ObjectSegment(size_t size) : Segment(SegmentType::OBJECT_SEGMENT), size(size)
    {
        objects_ = new StreamElement*[size]();
    }

    ~ObjectSegment()
    {
        delete[] objects_;
    }

    int putObject(int offset, StreamElement* record)
    {
        LOG("objects address" << objects_[offset]);
        LOG("objects size()" << size);
        objects_[offset] = record;
        return 1; // written size
    }

    void put(int index, const ObjectSegment* src, int offset, int length)
    {
        if (src == nullptr) {
            throw std::invalid_argument("Source ObjectSegment is null");
        }
        if (index < 0 || offset < 0 || length < 0 || static_cast<size_t>(index) + static_cast<size_t>(length) > size ||
            static_cast<size_t>(offset) + static_cast<size_t>(length) > src->size) {
            throw std::out_of_range("ObjectSegment copy range out of bounds");
        }
        int copied = 0;
        try {
            for (; copied < length; copied++) {
                objects_[index + copied] = CloneObject(src->objects_[offset + copied]);
            }
            ownsObjects_ = true;
        } catch (...) {
            for (int i = 0; i < copied; i++) {
                ReleaseObject(objects_[index + i]);
                objects_[index + i] = nullptr;
            }
            throw;
        }
    }

    void ReleaseObjects()
    {
        if (!ownsObjects_) {
            return;
        }
        for (size_t i = 0; i < size; i++) {
            ReleaseObject(objects_[i]);
            objects_[i] = nullptr;
        }
        ownsObjects_ = false;
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
    friend class ObjectSegmentChannelStateSerde;

    size_t size;
    bool ownsObjects_ = false;

    //  it is actually a  StreamRecord * [size] , allocate mem in constructor, StreamRecord.value are VectorBatch *
    //  notice in order to get high performance, the data related object are using raw pointer
    StreamElement** objects_;

    void ReleaseObjects(size_t offset, size_t length) noexcept
    {
        for (size_t i = 0; i < length; i++) {
            ReleaseObject(objects_[offset + i]);
            objects_[offset + i] = nullptr;
        }
    }

    static StreamElement* CloneObject(StreamElement* source)
    {
        if (source == nullptr) {
            return nullptr;
        }

        StreamElementTag tag = source->getTag();
        if (tag == StreamElementTag::TAG_UNKNOWN) {
            INFO_RELEASE("Warn: CloneObject tag is TAG_UNKNOWN");
            return nullptr;
        }
        if (tag == StreamElementTag::TAG_STREAM_STATUS || tag == StreamElementTag::TAG_RECORD_ATTRIBUTES ||
            tag == StreamElementTag::TAG_LATENCY_MARKER || tag == StreamElementTag::TAG_INTERNAL_WATERMARK) {
            ERROR_RELEASE("CloneObject does not support StreamElement tag " << static_cast<int>(tag));
            throw std::runtime_error(
                "CloneObject does not support StreamElement tag " + std::to_string(static_cast<int>(tag)));
        }
        if (tag == StreamElementTag::TAG_WATERMARK) {
            auto* watermark = dynamic_cast<Watermark*>(source);
            if (watermark == nullptr) {
                INFO_RELEASE("Error: CloneObject watermark is nullptr.");
                return nullptr;
            }
            return new Watermark(watermark->getTimestamp());
        }

        if (tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP || tag == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP ||
            tag == StreamElementTag::VECTOR_BATCH) {
            if (auto* record = dynamic_cast<StreamRecord*>(source); record != nullptr && record->hasExternalRow()) {
                INFO_RELEASE("Warn: CloneObject StreamRecord hasExternalRow, which is not supported.");
                throw std::invalid_argument("CloneObject StreamRecord hasExternalRow, which is not supported.");
            }
            auto* vectorBatch = static_cast<VectorBatch*>(source->getValue());
            if (vectorBatch == nullptr) {
                INFO_RELEASE("Warn: CloneObject getValue is not VectorBatch.");
                if (static_cast<Row*>(source->getValue())) {
                    INFO_RELEASE("Warn: CloneObject getValue is Row.");
                }
                return nullptr;
            }
            auto* copiedVectorBatch = vectorBatch->copy();
            if (auto* record = dynamic_cast<StreamRecord*>(source)) {
                StreamRecord* copiedRecord = record->hasTimestamp()
                                                 ? new StreamRecord(copiedVectorBatch, record->getTimestamp())
                                                 : new StreamRecord(copiedVectorBatch);
                copiedRecord->setExternalRow(record->hasExternalRow());
                copiedRecord->setTag(tag);
                return copiedRecord;
            }

            auto* copiedElement = new StreamElement(StreamElementTag::VECTOR_BATCH);
            copiedElement->setValue(copiedVectorBatch);
            return copiedElement;
        }

        ERROR_RELEASE("CloneObject encountered unsupported StreamElement tag " << static_cast<int>(tag));
        throw std::runtime_error(
            "CloneObject encountered unsupported StreamElement tag " + std::to_string(static_cast<int>(tag)));
    }

    static void ReleaseObject(StreamElement* object)
    {
        if (object == nullptr) {
            return;
        }

        StreamElementTag tag = object->getTag();
        if (tag == StreamElementTag::TAG_REC_WITH_TIMESTAMP || tag == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP ||
            tag == StreamElementTag::VECTOR_BATCH) {
            if (auto* record = dynamic_cast<StreamRecord*>(object)) {
                if (!record->hasExternalRow()) {
                    delete static_cast<VectorBatch*>(record->getValue());
                }
                delete record;
                return;
            }
            delete static_cast<VectorBatch*>(object->getValue());
            delete object;
            return;
        }

        if (tag == StreamElementTag::TAG_WATERMARK) {
            if (auto* watermark = dynamic_cast<Watermark*>(object)) {
                delete watermark;
            } else {
                delete object;
            }
            return;
        }

        delete object;
    }
};
} // namespace omnistream

#endif
