/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_INPUTCONVERSIONOPERATOR_H
#define OMNISTREAM_INPUTCONVERSIONOPERATOR_H

#include "table/runtime/operators/TableStreamOperator.h"
#include "core/operators/OneInputStreamOperator.h"

class InputConversionOperator : public TableStreamOperator<RowData*>, public OneInputStreamOperator {
public:
    InputConversionOperator(const nlohmann::json& config, Output* output)
        : output(output), description(config), propagateWatermark(false)
    {
        this->setOutput(output);
//        requiresWrapping = config["requiresWrapping"].get<bool>();
//        produceRowtimeMetadata = config["produceRowtimeMetadata"].get<bool>();
        propagateWatermark = false;
//        isInsertOnly = config["isInsertOnly"].get<bool>();
    }

    void open() override
    {
    }

    void ProcessWatermark(Watermark *mark) override
    {
        if (propagateWatermark || mark->getTimestamp() == INT64_MAX) {
            TableStreamOperator<RowData*>::ProcessWatermark(mark);
        }
    }

    void processBatch(StreamRecord *record) override
    {
        output->collect(record);
    }

    void processElement(StreamRecord *record) override
    {
        output->collect(record);
    }

    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override
    {
        output->emitWatermarkStatus(watermarkStatus);
    }

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override
    {
        LOG("InputConversionOperator initializeState()")
        // Do Nothing
    }

    std::string getTypeName() override
    {
        std::string typeName = "InputConversionOperator";
        typeName.append(__PRETTY_FUNCTION__) ;
        return typeName ;
    }

private:
    Output* output;
    nlohmann::json description;
//    bool requiresWrapping;
//    bool produceRowtimeMetadata;
    bool propagateWatermark;
//    bool isInsertOnly;
};

#endif // OMNISTREAM_INPUTCONVERSIONOPERATOR_H
