/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/1/25.
//

#include "ObjectSegmentFactory.h"
#include "vectorbatch/VectorBatch.h"

namespace omnistream {
    std::shared_ptr<ObjectSegment> omnistream::ObjectSegmentFactory::allocateUnpooledSegment(int segmentSize)
    {
        return std::make_shared<ObjectSegment>(segmentSize);
    }
}

