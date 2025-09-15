/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_NONTIMESTAMPCONTEXT_H
#define FLINK_TNEL_NONTIMESTAMPCONTEXT_H

#include "functions/SourceContext.h"

class NonTimestampContext : public SourceContext {
public:
    NonTimestampContext(Object *checkpointLock, Output *output, bool isStream = false) : lock(checkpointLock), output(output), isStream(isStream) {
        reuse = new StreamRecord();
    }

    ~NonTimestampContext() override {
        delete reuse;
    }

    void collect(void *element) override {
        lock->mutex.lock();
        if (isStream) {
            output->collect(reuse->replace(element));
        } else {
            output->collect(new StreamRecord(element));
        }
        lock->mutex.unlock();
    }

    void collectWithTimestamp(void *element, int64_t timestamp) override {
        // ignore the timestamp
        collect(element);
    }

    void emitWatermark(Watermark* mark) override {
        // do nothing
    }

    void markAsTemporarilyIdle() override {
        // do nothing
    }

    Object *getCheckpointLock() override {
        return lock;
    }

    void close() override {}

private:
    Object *lock;
    Output *output;
    StreamRecord *reuse;
    bool isStream;
};

#endif  //FLINK_TNEL_NONTIMESTAMPCONTEXT_H
