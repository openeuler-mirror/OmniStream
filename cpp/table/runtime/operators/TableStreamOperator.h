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

#ifndef FLINK_TNEL_TABLESTREAMOPERATOR_H
#define FLINK_TNEL_TABLESTREAMOPERATOR_H

#include "streaming/api/operators/AbstractStreamOperator.h"

template <typename OUT>
class TableStreamOperator : public AbstractStreamOperator<OUT> {
public:
    explicit TableStreamOperator(Output *output) : AbstractStreamOperator<OUT>(output) {}
    TableStreamOperator() {}

    void open() override
    {
        AbstractStreamOperator<OUT>::open();
        ctx = new ContextImpl(AbstractStreamOperator<OUT>::getProcessingTimeService());
    }

    void ProcessWatermark(Watermark *mark) override
    {
        if (this->timeServiceManager != nullptr) {
            this->timeServiceManager->template advanceWatermark<int64_t>(mark);
        }
        this->output->emitWatermark(mark);
        currentWatermark = mark->getTimestamp();
    }

protected:
    class ContextImpl {
    public:
        StreamRecord *element;
        explicit ContextImpl(ProcessingTimeService *timerService) : timerService(timerService) {};

        int64_t timestamp()
        {
            if (element == nullptr || !(element->hasTimestamp())) {
                return 0;
            } else {
                return element->getTimestamp();
            }
        };

    protected:
        ProcessingTimeService *timerService;
    };

    ContextImpl *ctx;
    int64_t currentWatermark = INT64_MIN;
};

#endif // FLINK_TNEL_TABLESTREAMOPERATOR_H
