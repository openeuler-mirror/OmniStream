/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 4/28/25.
//

#ifndef FLINK_TNEL_SWITCHINGONCLOSE_H
#define FLINK_TNEL_SWITCHINGONCLOSE_H

#include "functions/SourceContext.h"
#include "ClosedContext.h"

class SwitchingOnClose : public SourceContext {
public:
    explicit SwitchingOnClose(SourceContext *nestedContext) {
        this->nestedContext = nestedContext;
    }

    ~SwitchingOnClose() override {
        delete nestedContext;
    }

    void collect(void *element) override {
        nestedContext->collect(element);
    }

    void collectWithTimestamp(void *element, int64_t timestamp) override {
        nestedContext->collectWithTimestamp(element, timestamp);
    }

    void emitWatermark(Watermark* mark) override {
        nestedContext->emitWatermark(mark);
    }

    void markAsTemporarilyIdle() override {
        nestedContext->markAsTemporarilyIdle();
    }

    Object *getCheckpointLock() override {
        return nestedContext->getCheckpointLock();
    }

    void close() override{
        nestedContext->close();
        this->nestedContext = new ClosedContext(nestedContext->getCheckpointLock());
    }

private:
    SourceContext *nestedContext;
};

#endif  //FLINK_TNEL_SWITCHINGONCLOSE_H
