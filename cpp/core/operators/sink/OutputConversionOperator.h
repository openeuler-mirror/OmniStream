/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OUTPUTCONVERSIONOPERATOR_H
#define OMNISTREAM_OUTPUTCONVERSIONOPERATOR_H

#include <vector>
#include <memory>
#include <optional>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <limits>
#include "core/operators/OneInputStreamOperator.h"
#include "core/operators/TimestampedCollector.h"
#include "table/Row.h"

class OutputConversionOperator : public OneInputStreamOperator {
public:

    OutputConversionOperator(const nlohmann::json& config,
                             Output* output);

    void open();

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override {
    }

    void processElement(StreamRecord *record) override;

    void processBatch(StreamRecord *record) override;

    RowData* getEntireRow(omnistream::VectorBatch *batch, int rowId);

    Row* toExternal(RowData *internalRecord);

    Output* getOutput();

    void processWatermark(Watermark *watermark);

    void processWatermarkStatus(WatermarkStatus *watermarkStatus);

private:
    std::vector<std::string> inputTypes;
    std::vector<std::string> outputTypes;
    int rowtimeIndex;
    bool consumeRowtimeMetadata;
    StreamRecord* outRecord;
    Output* output;
    nlohmann::json description;
    TimestampedCollector* collector;
};

#endif // OMNISTREAM_OUTPUTCONVERSIONOPERATOR_H
