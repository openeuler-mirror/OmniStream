//
// Created by root on 12/27/25.
//

#ifndef OMNISTREAM_NETWORKBUFFERPOOL_H
#define OMNISTREAM_NETWORKBUFFERPOOL_H

#include "BufferPoolFactory.h"
#include "SegmentProvider.h"

namespace omnistream {
class NetworkBufferPool : public BufferPoolFactory, public SegmentProvider, public AvailabilityProvider {
public:
    virtual std::shared_ptr<CompletableFuture> GetAvailableFuture() = 0;
};
}

#endif //OMNISTREAM_NETWORKBUFFERPOOL_H
