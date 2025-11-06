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

#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include "BufferRecycler.h"
#include "BufferProvider.h"

namespace omnistream {

    class BufferPool : public BufferProvider, public BufferRecycler   {
    public:
        virtual void reserveSegments(int numberOfSegmentsToReserve)  = 0;
        virtual void lazyDestroy() = 0;
        virtual void cancel() = 0;
        bool isDestroyed() override = 0;
        virtual int getNumberOfRequiredSegments() const =0 ;
        virtual int getMaxNumberOfSegments() const =0 ;
        virtual int getNumBuffers() = 0;
        virtual void setNumBuffers(int numBuffers) =0 ;
        virtual int getNumberOfAvailableSegments()  = 0;
        virtual int bestEffortGetNumOfUsedBuffers() const = 0;

        virtual std::shared_ptr<Segment> requestSegment() = 0;
        virtual std::shared_ptr<Segment> requestSegment(int targetChannel) = 0;

        virtual std::shared_ptr<Segment> requestSegmentBlocking() = 0;
        virtual std::shared_ptr<Segment> requestSegmentBlocking(int targetChannel) = 0;
    };
}

#endif // BUFFERPOOL_H
