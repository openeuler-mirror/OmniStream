/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once

#include <string>
#include <cstdint>
#include <nlohmann/json.hpp>
#include <vector>  // Include the vector header
#include <iostream>
#include <fstream>
#include "common.h"
#include "../writer/RecordWriter.h"
#include "../operators/StreamOperator.h"
#include "outputbuffer.h"
#include "../graph/TaskPartitionerConfig.h"
#include "StreamTaskNetworkOutput.h"
#include "StreamTaskNetworkInput.h"
#include "../io/StreamOneInputProcessor.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "partitioner/StreamPartitionerV2.h"
#include "KeySelector.h"
#include "tasks/OperatorChain.h"

using json = nlohmann::json;
// class StreamOneInputProcessor;

namespace omnistream::datastream {
class StreamTask {
public:
    StreamTask(const nlohmann::json& ntdd, void* outputBufferStatus,
               std::shared_ptr<RuntimeEnvironmentV2> runtimeEnv);  // Default constructor

    ~StreamTask();  // Destructor
    void cleanUp();

    // entry for native
    StreamTaskStateInitializerImpl* createStreamTaskStateInitializer();

    uint32_t emitNextRecord();

    void clearOutputBuffer();

    OutputBufferStatus* getOutputBufferStatus();

    StreamOneInputProcessor* createOmniInputProcessor(const json& channelInfo, int operatorMethodIndicator);

    void addStreamOneInputProcessor(StreamOneInputProcessor* processor);

    StreamOneInputProcessor* getStreamOneInputProcessor(size_t index);

    void resetOutputBufferAndRecordWriter();

    StreamOperator* getMainOperator()
    {
        return mainOperator_;
    }

    omnistream::datastream::StreamPartitioner<IOReadableWritable>* createPartitioner();

    static TaskPartitionerConfig extractTaskPartitionerConfig(const json& ntdd);
    void setTaskPartitionerConfig(TaskPartitionerConfig taskPartitionerConfig_)
    {
        taskPartitionerConfig = taskPartitionerConfig_;
    }

private:
    // owning
    std::shared_ptr<RuntimeEnvironmentV2> env_;
    std::vector<OperatorConfig> operatorChainConfig_;

    // need read some more code.
    std::shared_ptr<RecordWriterDelegate> recordWriter_;
    // owning
    omnistream::OperatorChainV2* operatorChain_;

    // weal ref. mainOperator_ is only a shortcut of first operator in operator chain. should be deleted by operatroChain_
    StreamOperator* mainOperator_;

    // weak ref, the objet is owned by java object
    OutputBufferStatus* outputBufferStatus_;

    TaskPartitionerConfig taskPartitionerConfig;

    // owning, although some mem function return the pointer to outside of class
    std::vector<StreamOneInputProcessor*> streamOneInputProcessors;

    // set up ////////////////////////////////////////set up
    // ntdd  Native task deployment descriptor
    // return int error code, 0 is for success
    int createTask(const json& ntdd);

    // convert the json ntdd to object ndd
    static int extractOperatorChainConfig(std::vector<OperatorConfig>&, const json& ntdd);

    int setupRecordWriter();
    int setupOperatorChain(std::vector<OperatorConfig>&);
    std::unique_ptr<StreamTaskNetworkOutput> createDataOutput(int operatorMethodIndicator);
    std::unique_ptr<StreamTaskNetworkInput> createDataInput(std::vector<long>& channelInfos);

    std::shared_ptr<RecordWriterDelegate> createRecordWriterDelegate(const TaskInformationPOD& taskConfig);

    std::vector<RecordWriter*> createRecordWriters(const TaskInformationPOD& taskConfig);
    RecordWriter* createRecordWriter(const StreamEdgePOD& edge);

    template <typename K>
    KeySelector<K>* buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields);

    // partitioner
    StreamPartitioner<IOReadableWritable >* createPartitionerFromDesc(const StreamEdgePOD& edge);

};
}  // namespace omnistream::datastream
