/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OPERATORCHAIN_V2_H
#define OPERATORCHAIN_V2_H
#include <executiongraph/StreamConfigPOD.h>
#include <executiongraph/StreamEdgePOD.h>
#include <executiongraph/operatorchain/OperatorChainPOD.h>
#include <io/RecordWriterOutputV2.h>
#include <io/RecordWriterOutput.h>
#include <operators/StreamOperatorFactory.h>
#include <operators/StreamOperator.h>
#include <task/StreamOperatorWrapper.h>
#include <typeinfo/TypeInformation.h>
#include "io/writer/RecordWriterDelegateV2.h"
#include "writer/RecordWriterDelegate.h"
#include <runtime/io/writer/RecordWriterDelegateV2.h>
#include "CopyingBroadcastingOutputCollector.h"
#include "taskmanager/OmniRuntimeEnvironment.h"

#include "tasks/mailbox/StreamTaskActionExecutor.h"
#include "runtime/operators/TableOperatorConstants.h"
#include "runtime/operators/coordination/OperatorEventHandler.h"

namespace omnistream {

    // forward declaration
    class OmniStreamTask;

    class OperatorChainV2 {
    public:
        OperatorChainV2(std::weak_ptr<OmniStreamTask> containingTask,
                        std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate);

        OperatorChainV2(std::shared_ptr<RuntimeEnvironmentV2> env,
                        std::shared_ptr<omnistream::datastream::RecordWriterDelegate> recordWriterDelegate)
        {
            TaskInformationPOD taskConfiguration = env->taskConfiguration();

            auto configuration = taskConfiguration.getStreamConfigPOD();
            auto outputsInOrder = configuration.getOutEdgesInOrder();
            auto chainedConfigs = taskConfiguration.getChainedConfigMap();

            std::unordered_map<int, datastream::RecordWriterOutput*> recordWriterOutputs;
            createDataStreamChainOutputs(outputsInOrder, chainedConfigs, recordWriterDelegate, recordWriterOutputs);

            std::vector<StreamOperatorWrapper *> allOperatorWrappers;
            this->mainOperatorOutput = createDataStreamOutputCollector(configuration, chainedConfigs, recordWriterOutputs, allOperatorWrappers);

            auto opDesc = configuration.getOperatorDescription();
            auto chainedOperator = StreamOperatorFactory::createOperatorAndCollector(opDesc, mainOperatorOutput, nullptr);
            auto operatorWrapper = new StreamOperatorWrapper(chainedOperator, false);
            this->mainOperatorWrapper = operatorWrapper;
            allOperatorWrappers.emplace_back(operatorWrapper);
            if (allOperatorWrappers.size() != 0) {
                this->tailOperatorWrapper = allOperatorWrappers[0];
            } else {
                this->tailOperatorWrapper = mainOperatorWrapper;
            }
            linkOperatorWrappers(allOperatorWrappers);
        }

        ~OperatorChainV2() {};
    void finishOperators(StreamTaskActionExecutor *actionExecutor);

    StreamOperator* createMainOperatorAndCollector(OperatorChainPOD& opChainConfig,
        RecordWriterOutputV2* chainOutput);

    void initializeStateAndOpenOperators(StreamTaskStateInitializerImpl *initializer);

    [[nodiscard]] StreamOperator* getMainOperator() const
    {
        return mainOperatorWrapper->getStreamOperator();
    }

    [[nodiscard]] WatermarkGaugeExposingOutput* GetMainOperatorOutput() const
    {
        return mainOperatorOutput;
    }

    void dispatchOperatorEvent(const std::string& operatorIdString, const std::string& eventString);

    protected:
    // WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;
    WatermarkGaugeExposingOutput* mainOperatorOutput;

    std::unordered_map<std::string, OperatorEventHandler*> handlers;

    // ownership is mainOperatorWrapper own its next, wrapper also own its op, op own its output
    StreamOperatorWrapper* mainOperatorWrapper;

        // weak ref,
        StreamOperatorWrapper *tailOperatorWrapper;

    private:
    // future the following function should be private and the logic will be refactory
    void createChainOutputs(std::vector<StreamEdgePOD>& outputsInOrder,
    std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
        std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate,
        std::unordered_map<int, RecordWriterOutputV2*>& recordWriterOutputs);
    RecordWriterOutputV2* createStreamOutput(RecordWriterV2* recordWriter, TypeInformation& typeInformation);

        void createDataStreamChainOutputs(std::vector<StreamEdgePOD>& outputsInOrder, std::unordered_map<int, StreamConfigPOD>& chainedConfigs,
                                          std::shared_ptr<datastream::RecordWriterDelegate> recordWriterDelegate, std::unordered_map<int, datastream::RecordWriterOutput*>& recordWriterOutputs);
        datastream::RecordWriterOutput* createDataStreamStreamOutput(datastream::RecordWriter* recordWriter, TypeInformation& typeInformation);

    TypeInformation* getChainOutputType(OperatorPOD operatorPod);
    static WatermarkGaugeExposingOutput* wrapOperatorIntoOutput(StreamOperator* op,
                                                                    omnistream::OperatorPOD &opConfig);
    WatermarkGaugeExposingOutput *createOperatorChain(
            const std::shared_ptr<OmniStreamTask>& streamTask,
            StreamConfigPOD *operatorConfig,
            std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
            std::unordered_map<int, RecordWriterOutputV2 *> &recordWriterOutputs,
            std::vector<StreamOperatorWrapper *> &allOperatorWrappers);

        WatermarkGaugeExposingOutput* createOutputCollector(
            const std::shared_ptr<OmniStreamTask>& streamTask, StreamConfigPOD &operatorConfig,
            std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
            std::unordered_map<int, RecordWriterOutputV2 *> &recordWriterOutputs,
            std::vector<StreamOperatorWrapper *> &allOperatorWrappers);
        static void linkOperatorWrappers(std::vector<StreamOperatorWrapper*>& allOperatorWrappers);

    WatermarkGaugeExposingOutput* createDataStreamOutputCollector(StreamConfigPOD &operatorConfig,
                                                         std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
                                                         std::unordered_map<int, datastream::RecordWriterOutput *> &recordWriterOutputs,
                                                         std::vector<StreamOperatorWrapper *> &allOperatorWrappers);
    WatermarkGaugeExposingOutput *createDataStreamOperatorChain(StreamConfigPOD &operatorConfig,
                                                          std::unordered_map<int, StreamConfigPOD> &chainedConfigs,
                                                          std::unordered_map<int, datastream::RecordWriterOutput *> &recordWriterOutputs,
                                                          std::vector<StreamOperatorWrapper *> &allOperatorWrappers);

    ReadIterator getAllOperators(bool reverse)
    {
        return reverse
                ? ReadIterator(tailOperatorWrapper, true)
                : ReadIterator(mainOperatorWrapper, false);
    }

    void registerHandler(OperatorPOD& opDesc, StreamOperator* streamOperator)
    {
        auto id = opDesc.getId();
        if (id == OPERATOR_NAME_SOURCE_OPERATOR) {
            if (auto* handler = dynamic_cast<OperatorEventHandler*>(streamOperator)) {
                handlers.emplace(opDesc.getOperatorId(), handler);
            } else {
                LOG("Unsupported source operator of csv");
            }
        }
    }

    // std::vector<OperatorConfig> & opChainConfig_;
    // std::shared_ptr<RecordWriterDelegate> recordWriterDelegate_;
    };
}
#endif //OPERATORCHAIN_H
/**
*
    // future the following function should be private and the logic will be refactory
    RecordWriterOutput* createChainOutputs(NonChainedOutput * streamOutput, std::shared_ptr<RecordWriterDelegate> recordWriterDelegate, std::vector<OperatorConfig> & opChainConfig );


    RecordWriterOutput* createStreamOutput(RecordWriter* recordWriter, NonChainedOutput * streamOutput, TypeInformation & typeInformation );

    TypeInformation*  getChainOutputType (std::vector<OperatorConfig> &opChainConfig);

    // TBD simplified, assume we have one on op now
    StreamOperator * createMainOperatorAndCollector( std::vector<OperatorConfig> & opChainConfig, RecordWriterOutput* chainOutput);
    void initializeStateAndOpenOperators(StreamTaskStateInitializerImpl *initializer);
protected:
    // WatermarkGaugeExposingOutput<StreamRecord<OUT>> mainOperatorOutput;
    WatermarkGaugeExposingOutput * mainOperatorOutput{};

    //ownership is mainOperatorWrapper own its next, wrapper also own its op, op own its output
    StreamOperatorWrapper * mainOperatorWrapper;

    //weak ref,
    StreamOperatorWrapper * tailOperatorWrapper;

private:
    std::vector<OperatorConfig> & opChainConfig_;
    std::shared_ptr<RecordWriterDelegate> recordWriterDelegate_;
 */