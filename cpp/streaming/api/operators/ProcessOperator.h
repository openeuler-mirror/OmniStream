/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_PROCESSOPERATOR_H
#define FLINK_TNEL_PROCESSOPERATOR_H

#include "core/operators/OneInputStreamOperator.h"
#include "core/operators/AbstractUdfStreamOperator.h"
#include "../functions/ProcessFunction.h"
#include "core/operators/TimestampedCollector.h"

// This class is not keyed, but AbstractUdfStreamOperator's base AbtractStreamOperator is, so I will give it a fake K=int
// LookupJoin's ProcessFunction<IN, OUT> is LookupJoinRunner<VectorBatch*, VectorBatch*>
class ProcessOperator : public OneInputStreamOperator, public AbstractUdfStreamOperator<ProcessFunction, int> {
public:
    ProcessOperator(ProcessFunction* function, const nlohmann::json& description, Output* output) :
        AbstractUdfStreamOperator<ProcessFunction, int>(function) {
        this->output = output;
        this->chainingStrategy = ChainingStrategy::ALWAYS;
    };
    void open() override {
        // ProcessFunction is opened here
        AbstractUdfStreamOperator<ProcessFunction, int>::open();
        collector = new TimestampedCollector(this->output);
        context = new ContextImpl(this->userFunction, this->getProcessingTimeService(), this);
    }

    void processElement(StreamRecord *element) override {
        NOT_IMPL_EXCEPTION;
    }
    void processBatch(StreamRecord *element) {
        this->userFunction->processBatch(reinterpret_cast<omnistream::VectorBatch*>(element->getValue()), context, collector);
    }

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override {}

    void ProcessWatermark(Watermark* mark) override {
        AbstractStreamOperator::ProcessWatermark(mark);
        this->currentWatermark_ = mark->getTimestamp();
    }
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override {
        output->emitWatermarkStatus(watermarkStatus);
    }
private:
    class ContextImpl : public ProcessFunction::Context, public TimerService {
    public:
        ContextImpl(ProcessFunction* function, ProcessingTimeService* timeService, ProcessOperator* op_) :
            processingTimeService(timeService), op(op_) {
            op->userFunction = function;
        }
        int64_t timestamp() override {
            return {};
        }
        TimerService* timerService() override {
            return this;
        }
        /*
        void Output(OutputTag* tag, omnistream::VectorBatch* value) {
        }
         */
        int64_t currentProcessingTime() const {
            return processingTimeService->getCurrentProcessingTime();
        }
        int64_t currentWatermark() const {
            return op->currentWatermark_;
        }
        // Flink don't have the following 2 function here.
        // They exist because of the existence of different TimerServeice class
        void registerTimer(int64_t timestamp, ProcessingTimeCallback *target) override {
            NOT_IMPL_EXCEPTION
        };
        int64_t getCurrentProcessingTime() override {
            NOT_IMPL_EXCEPTION
        };
    private:
        ProcessingTimeService* processingTimeService;
        ProcessOperator* op;
    };

    TimestampedCollector* collector;
    int64_t currentWatermark_ = std::numeric_limits<int64_t>::min();
    ContextImpl* context;
};
#endif // FLINK_TNEL_PROCESSOPERATOR_H
