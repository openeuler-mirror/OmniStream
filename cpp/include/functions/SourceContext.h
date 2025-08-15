/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef OMNIFLINK_SOURCECONTEXT_H
#define OMNIFLINK_SOURCECONTEXT_H
#include "basictypes/Object.h"
#include "Watermark.h"
class SourceContext {
public:
    virtual ~SourceContext() = default;

    // Collect method
    virtual void collect(void *element) = 0;

    virtual void collectWithTimestamp(void* var1, int64_t var2) = 0;

    virtual void emitWatermark(Watermark* var1) = 0;

    virtual void markAsTemporarilyIdle() = 0;

    virtual void close() = 0;

    virtual Object* getCheckpointLock() = 0;
};

#endif //OMNIFLINK_SOURCECONTEXT_H