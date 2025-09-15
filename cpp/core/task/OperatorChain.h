/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
/*

#ifndef FLINK_TNEL_OPERATORCHAIN_H
#define FLINK_TNEL_OPERATORCHAIN_H

#include "../include/common.h"
#include "../graph/OperatorChainConfig.h"
#include "../graph/NonChainedOutput.h"
#include "WatermarkGaugeExposingOutput.h"
#include "StreamOperatorWrapper.h"
#include "../writer/RecordWriterDelegate.h"
#include "../io/RecordWriterOutput.h"
#include "../typeinfo/TypeInformation.h"
#include "../operators/OneInputStreamOperator.h"
#include "core/operators/AbstractStreamOperator.h"
using namespace omnistream;

*/
/***
***Obsoleted, DO NOT USE IT AT ALL
Obsoleted, DO NOT USE IT AT ALL
Obsoleted, DO NOT USE IT AT ALL
*//*


class OperatorChain {
public:
    OperatorChain(std::vector<OperatorConfig> &opChainConfig) : opChainConfig_(opChainConfig)
    {
        mainOperatorWrapper = nullptr;
        tailOperatorWrapper = nullptr;
    }
    OperatorChain(
        std::vector<OperatorConfig> &opChainConfig, std::shared_ptr<RecordWriterDelegate> recordWriterDelegate);
    ~OperatorChain();

    // future the following function should be private and the logic will be refactory
    RecordWriterOutput *createChainOutputs(NonChainedOutput *streamOutput,
        std::shared_ptr<RecordWriterDelegate> recordWriterDelegate, std::vector<OperatorConfig> &opChainConfig);

    RecordWriterOutput *createStreamOutput(
        RecordWriter *recordWriter, NonChainedOutput *streamOutput, TypeInformation &typeInformation);

    TypeInformation *getChainOutputType(std::vector<OperatorConfig> &opChainConfig);

    // TBD simplified, assume we have one on op now
    StreamOperator *createMainOperatorAndCollector(
        std::vector<OperatorConfig> &opChainConfig, RecordWriterOutput *chainOutput);
    void initializeStateAndOpenOperators(StreamTaskStateInitializerImpl *initializer);

protected:
    // WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;
    WatermarkGaugeExposingOutput *mainOperatorOutput{};

    // ownership is mainOperatorWrapper own its next, wrapper also own its op, op own its output
    StreamOperatorWrapper *mainOperatorWrapper;

    // weak ref,
    StreamOperatorWrapper *tailOperatorWrapper;

private:
    std::vector<OperatorConfig> &opChainConfig_;
    std::shared_ptr<RecordWriterDelegate> recordWriterDelegate_;
};

#endif  // FLINK_TNEL_OPERATORCHAIN_H
*/
