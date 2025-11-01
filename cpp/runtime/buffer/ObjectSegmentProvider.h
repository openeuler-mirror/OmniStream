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

        virtual std::string toString() const
        {
            std::stringstream ss;
            ss << "ObjectSegmentProvider";
            return ss.str();
        }
    };


} // namespace omnistream

#endif // OMNISTREAM_OBJECTSEGMENTPROVIDER_H