/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OBJECTSEGMENTFACTORY_H
#define OBJECTSEGMENTFACTORY_H
#include <memory>
#include <buffer/ObjectSegment.h>

namespace omnistream
{
    class ObjectSegmentFactory {
    public:
        static std::shared_ptr<ObjectSegment> allocateUnpooledSegment(int segmentSize);
    };
}


#endif //OBJECTSEGMENTFACTORY_H
