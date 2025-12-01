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

#ifndef FLINK_TNEL_SWITCHINGONCLOSE_H
#define FLINK_TNEL_SWITCHINGONCLOSE_H

#include "functions/SourceContext.h"
#include "ClosedContext.h"

class SwitchingOnClose : public SourceContext {
public:
    explicit SwitchingOnClose(SourceContext *nestedContext)
    {
        this->nestedContext = nestedContext;
    }

    ~SwitchingOnClose() override
    {
        delete nestedContext;
    }

    void collect(void *element) override
    {
        nestedContext->collect(element);
    }

    void collectWithTimestamp(void *element, int64_t timestamp) override
    {
        nestedContext->collectWithTimestamp(element, timestamp);
    }

    void emitWatermark(Watermark* mark) override
    {
        nestedContext->emitWatermark(mark);
    }

    void markAsTemporarilyIdle() override
    {
        nestedContext->markAsTemporarilyIdle();
    }

    Object *getCheckpointLock() override
    {
        return nestedContext->getCheckpointLock();
    }

    void close() override
    {
        nestedContext->close();
        if (nestedContext) {
            delete nestedContext;
        }
        nestedContext = nullptr;
    }

private:
    SourceContext *nestedContext;
};

#endif  // FLINK_TNEL_SWITCHINGONCLOSE_H
