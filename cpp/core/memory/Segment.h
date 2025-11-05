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

#ifndef SEGMENT_H
#define SEGMENT_H

enum class SegmentType {
    NONE,
    OBJECT_SEGMENT,
    MEMORY_SEGMENT
};


class Segment {
public:
    explicit Segment(const SegmentType& segment_type)
        : segmentType(segment_type)
    {
    }

    virtual ~Segment() = default;

    [[nodiscard]] bool isObjectSegment() const
    {
        return segmentType == SegmentType::OBJECT_SEGMENT;
    }

    [[nodiscard]] bool isMemorySegment() const
    {
        return segmentType == SegmentType::MEMORY_SEGMENT;
    }

private:
    SegmentType segmentType;
};


#endif // SEGMENT_H
