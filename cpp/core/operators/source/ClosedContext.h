/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_CLOSEDCONTEXT_H
#define FLINK_TNEL_CLOSEDCONTEXT_H

#include "functions/SourceContext.h"

class ClosedContext : public SourceContext {
public:
    explicit ClosedContext(Object *checkpointLock) {
        this->checkpointLock = checkpointLock;
    }

    void collectWithTimestamp(void *element, int64_t timestamp) override {
        NOT_IMPL_EXCEPTION;
    }

    void collect(void *element) override {
        NOT_IMPL_EXCEPTION;
    }

    void emitWatermark(Watermark *mark) override {
        NOT_IMPL_EXCEPTION;
    }

    void markAsTemporarilyIdle() override {
        NOT_IMPL_EXCEPTION;
    }

    Object *getCheckpointLock() override {
        return checkpointLock;
    }

    void close() override {
        // nothing to be done
    }

private:
    Object *checkpointLock;
};

#endif  //FLINK_TNEL_CLOSEDCONTEXT_H
