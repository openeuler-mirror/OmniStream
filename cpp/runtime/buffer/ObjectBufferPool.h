/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OBJECTBUFFERPOOL_H
#define OMNISTREAM_OBJECTBUFFERPOOL_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "ObjectBufferProvider.h"
#include "ObjectBufferRecycler.h"
#include "ObjectBuffer.h"
#include "ObjectSegment.h"

namespace omnistream {

    class ObjectBufferPool : public ObjectBufferProvider, public ObjectBufferRecycler {
    public:
        ObjectBufferPool() = default;
        ~ObjectBufferPool() override = default;

        virtual void reserveSegments(int numberOfSegmentsToReserve)  = 0;
        virtual void lazyDestroy()  = 0;
        bool isDestroyed() override =0 ;
        virtual int getNumberOfRequiredObjectSegments() const =0 ;
        virtual int getMaxNumberOfObjectSegments() const =0 ;
        virtual int getNumBuffers()  =0 ;
        virtual void setNumBuffers(int numBuffers) =0 ;
        virtual int getNumberOfAvailableObjectSegments()  =0 ;
        virtual int bestEffortGetNumOfUsedBuffers() const =0 ;
        virtual std::shared_ptr<ObjectSegment> requestObjectSegment() = 0;
        virtual std::shared_ptr<ObjectSegment> requestObjectSegmentBlocking() =0;
        std::string toString() const override =0 ;
    };

} // namespace omnistream

#endif // OMNISTREAM_OBJECTBUFFERPOOL_H
