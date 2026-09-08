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

#include "ObjectSegment.h"

#include <algorithm>
#include <atomic>

#include "core/include/common.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "table/data/vectorbatch/VectorBatch.h"

namespace omnistream {

namespace {
std::atomic<long> g_segCreated{0};
std::atomic<long> g_segDestroyed{0};
std::atomic<long> g_stored{0};    // putObject calls
std::atomic<long> g_drained{0};   // elements actually taken by a consumer
}

void ObjectSegment::countDrained()
{
    g_drained.fetch_add(1, std::memory_order_relaxed);
}

void ObjectSegment::reportCounters(const char* where)
{
    long stored = g_stored.load();
    long drained = g_drained.load();
    INFO_RELEASE("OBJSEG_COUNTERS[" << where << "] created=" << g_segCreated.load()
        << " destroyed=" << g_segDestroyed.load()
        << " stored=" << stored << " drained=" << drained
        << " undrained=" << (stored - drained));
}

ObjectSegment::ObjectSegment(size_t size)
    : Segment(SegmentType::OBJECT_SEGMENT), size(size), objects_(new StreamElement*[size]())
{
    g_segCreated.fetch_add(1, std::memory_order_relaxed);
}

ObjectSegment::~ObjectSegment()
{
    g_segDestroyed.fetch_add(1, std::memory_order_relaxed);
    delete[] objects_;
}

int ObjectSegment::putObject(int offset, StreamElement* record)
{
    // if (offset == 0) {
    //     reset();
    // }

    LOG("objects address" << objects_[offset]);
    LOG("objects size()" << size);

    objects_[offset] = record;
    g_stored.fetch_add(1, std::memory_order_relaxed);
    sizeInBytes_ += calculateStoredObjectSizeInBytes(record);
    return 1;
}

StreamElement* ObjectSegment::getObject(int offset)
{
    return objects_[offset];
}

size_t ObjectSegment::getSize() const
{
    return size;
}

int64_t ObjectSegment::getObjectSizeInBytes() const
{
    return sizeInBytes_;
}

void ObjectSegment::reset()
{
    sizeInBytes_ = 0;
    capacityInBytes_ = 0;
    //reset elements in objects
    std::fill(objects_, objects_ + size, nullptr);
}

int64_t ObjectSegment::calculateStoredObjectSizeInBytes(const StreamElement* record)
{
    // A watermark is not a StreamRecord and its getValue() is not a VectorBatch, so it must be
    // handled before the cast below. It carries only a timestamp, so account a fixed size for it.
    if (record->getTag() == StreamElementTag::TAG_WATERMARK) {
        return static_cast<int64_t>(sizeof(int64_t));
    }
    // ObjectSegment is used by the vector-batch transport path, so only StreamRecord payloads
    // contribute bytes here and they are expected to be VectorBatch instances.
    auto* vectorBatch = static_cast<const VectorBatch*>(record->getValue());
    return vectorBatch == nullptr ? 0 : vectorBatch->getSizeInBytes();
}

void ObjectSegment::setCapacity(int64_t capacity)
{
    capacityInBytes_ = capacity;
}

int64_t ObjectSegment::getCapacity()
{
    return capacityInBytes_;
}


} // namespace omnistream
