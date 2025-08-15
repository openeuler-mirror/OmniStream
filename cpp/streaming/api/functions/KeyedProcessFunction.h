/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_KEYED_PROCESS_FUNCTION_H
#define FLINK_TNEL_KEYED_PROCESS_FUNCTION_H

#include "functions/AbstractRichFunction.h"
#include "core/operators/TimestampedCollector.h"
#include "table/data/utils/JoinedRowData.h"
#include "table/vectorbatch/VectorBatch.h"
template <typename K, typename I, typename O>
class KeyedProcessFunction : public AbstractRichFunction {
public:
    class Context {
    public:
        virtual ~Context() = default;
        virtual long timestamp() const = 0;
        virtual K getCurrentKey() const = 0;
        virtual void setCurrentKey(K& key) = 0;
    };

    class OnTimerContext : public Context {
    public:
        virtual ~OnTimerContext() = default;
        virtual K getCurrentKey() const override = 0;
    };
    // virtual void open(OpenContext* openContext) = 0;

    virtual void processElement(RowData& input, Context& ctx, TimestampedCollector& out) = 0;
    virtual void processBatch(omnistream::VectorBatch* inputBatch, Context& ctx, TimestampedCollector& out) = 0;
    virtual JoinedRowData* getResultRow() = 0;

    ~KeyedProcessFunction() = default;
    virtual ValueState<K>* getValueState() =0;
};

#endif  // FLINK_TNEL_KEYED_PROCESS_FUNCTION_H
