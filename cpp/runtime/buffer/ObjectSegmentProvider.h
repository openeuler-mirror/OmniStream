/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OBJECTSEGMENTPROVIDER_H
#define OMNISTREAM_OBJECTSEGMENTPROVIDER_H

#include <vector>
#include <memory>
#include <sstream>
#include <iostream>

namespace omnistream {

    class ObjectSegment;

    class ObjectSegmentProvider {
    public:
        virtual ~ObjectSegmentProvider() = default;

        virtual std::vector<std::shared_ptr<ObjectSegment>> requestUnpooledMemorySegments(int numberOfSegmentsToRequest) = 0;
        virtual void recycleUnpooledMemorySegments(const std::vector<std::shared_ptr<ObjectSegment>>& segments) = 0;

        virtual std::string toString() const {
            std::stringstream ss;
            ss << "ObjectSegmentProvider";
            return ss.str();
        }
    };


} // namespace omnistream

#endif // OMNISTREAM_OBJECTSEGMENTPROVIDER_H