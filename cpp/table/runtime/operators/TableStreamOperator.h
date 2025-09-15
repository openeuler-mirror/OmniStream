/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TABLESTREAMOPERATOR_H
#define FLINK_TNEL_TABLESTREAMOPERATOR_H

#include "core/operators/AbstractStreamOperator.h"

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

    void ProcessWatermark(Watermark *mark)
    {
        AbstractStreamOperator<OUT>::ProcessWatermark(mark);
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
