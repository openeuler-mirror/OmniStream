//
// Created by root on 12/31/25.
//

#ifndef OMNISTREAM_SEGMENTPROVIDER_H
#define OMNISTREAM_SEGMENTPROVIDER_H
#include <iostream>
#include <vector>
#include <memory>
#include "core/memory/MemorySegment.h"
#include "ObjectSegment.h"

namespace omnistream {
    class SegmentProvider {
    public:
        virtual std::vector<MemorySegment *> requestUnpooledMemorySegments(int numberOfSegmentsToRequest) = 0;

        virtual void recycleUnpooledMemorySegments(const std::vector<MemorySegment *> &segments) = 0;

        virtual std::vector<ObjectSegment *> requestUnpooledObjectSegments(int numberOfSegmentsToRequest) = 0;

        virtual void recycleUnpooledObjectSegments(const std::vector<ObjectSegment *> &segments) = 0;
    };
}
#endif //OMNISTREAM_SEGMENTPROVIDER_H
