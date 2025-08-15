/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "StreamTask.h"
#include "writer/SingleRecordWriter.h"
#include "writer/NonRecordWriter.h"
#include "writer/MultipleRecordWriters.h"
#include "../typeinfo/TypeInfoFactory.h"
#include "../partition/RebalancePartitioner.h"
#include "../partition/RescalePartitioner.h"
#include "../partition/GlobalPartitioner.h"
#include "../partition/KeyGroupStreamPartitioner.h"
#include "../io/StreamTaskNetworkInputFactory.h"
#include "partition/ForwardPartitioner.h"
#include "taskmanager/RuntimeEnvironment.h"
#include <filesystem>
#include "api/common/TaskInfoImpl.h"

namespace omnistream::datastream {
    StreamTask::StreamTask(const nlohmann::json &ntdd, void *outputBufferStatus, std::shared_ptr<RuntimeEnvironmentV2> env)
        : env_(env), outputBufferStatus_(reinterpret_cast<OutputBufferStatus *>(outputBufferStatus)), taskPartitionerConfig(extractTaskPartitionerConfig(ntdd))
    {
        createTask(ntdd);
    }


    StreamTask::~StreamTask() {}

    void StreamTask::cleanUp()
    {
        delete operatorChain_;
    }

// setup
    int StreamTask::createTask(const json &ntdd)
    {
        LOG(">>>Begin of createTask")

        // json ntdd to object tdd
        extractOperatorChainConfig(operatorChainConfig_, ntdd);

        LOG(">>>Setup RecordWriter")
        setupRecordWriter();

        LOG(">>>Setup OperatorChain")
        setupOperatorChain(operatorChainConfig_);

        LOG(">>> End of create StreamTask ")
        return 0;
    };

    int StreamTask::setupRecordWriter()
    {
        LOG("setupRecordWriter outputBuffer_ address " + std::to_string(outputBufferStatus_->outputBuffer_) +
            " capacity " + std::to_string(static_cast<int>(outputBufferStatus_->capacity_)));
        TaskInformationPOD taskConfig = env_->taskConfiguration();

        recordWriter_ = createRecordWriterDelegate(taskConfig);
        return 0;
    };

    int StreamTask::setupOperatorChain(std::vector<OperatorConfig> & operatorChainConfig)
    {
        operatorChain_ = new omnistream::OperatorChainV2(env_, recordWriter_);
        LOG("After after OperatorChain ")
        TaskInformationPOD taskConfiguration_ = env_->taskConfiguration();

        StreamTaskStateInitializerImpl *initializer =
                new StreamTaskStateInitializerImpl(
                    new RuntimeEnvironment(
                        new TaskInfoImpl(taskConfiguration_.getTaskName(),
                        taskConfiguration_.getMaxNumberOfSubtasks(), taskConfiguration_.getNumberOfSubtasks(),
                        taskConfiguration_.getIndexOfSubtask()),
                        new ExecutionConfig(env_->jobConfiguration().getAutoWatermarkInterval())));
        operatorChain_->initializeStateAndOpenOperators(initializer);
        mainOperator_ = operatorChain_->getMainOperator();

        LOG("After createMainOperatorAndCollector "  + std::to_string(reinterpret_cast<long> (mainOperator_)))

        return 0;
    }

    int StreamTask::extractOperatorChainConfig(std::vector<OperatorConfig> &chainConfig, const json &ntdd)
    {
        // Check types
        if (ntdd["operators"].is_array())
        {
            for (const auto &element : ntdd["operators"])
            {
                json inputType = element["inputs"]; // can be kind:basic or kind:row
                const json &outputType = element["output"];

                std::string name = element["name"];
                std::string uniqueName = element["id"];
                json description = element["description"];
                OperatorConfig opConfig(uniqueName, name, inputType, outputType, description);
                chainConfig.push_back(std::move(opConfig));
            }
        } else {
            THROW_LOGIC_EXCEPTION("NTDD JSON object is not well formed : " + to_string(ntdd))
        }

        return 0;
    }

    TaskPartitionerConfig StreamTask::extractTaskPartitionerConfig(const json &ntdd)
    {
        if (ntdd.contains("partition"))
        {
            json options = nullptr;
            json partitionInfo = ntdd["partition"];
            std::string partitionName = partitionInfo["partitionName"];
            int numberOfChannel = partitionInfo["channelNumber"];
            if (partitionName == "hash")
            {
                options = partitionInfo["hashFields"];
            }

            return TaskPartitionerConfig(partitionName, numberOfChannel, options);
        } else {
            // todo
            LOG("the partition info is missing.......")
            // Temporary dummy partition info for sink vertex
            return TaskPartitionerConfig("forward", 1, nullptr);
            THROW_LOGIC_EXCEPTION("THE partition info does not exist in ntdd: " + to_string(ntdd))
        }
    }

    std::unique_ptr<StreamTaskNetworkInput> StreamTask::createDataInput(std::vector<long> &chanelInfos)
    {
        LOG(">>>>> Begin")
        auto opConfig = operatorChainConfig_[0];
        auto inputTypes = opConfig.getInputType();
        nlohmann::json inputType;
        if (inputTypes.is_array()) {
            // uint32_t channelIndex = 0;//currently, channelInfo is not used
            uint32_t channelGateIndex = 0;
            //channelIndex = static_cast<uint32_t>(chanelInfos[0] & 0xFFFFFFFF);//currently, channelIndex is not used
            channelGateIndex = static_cast<uint32_t>((chanelInfos[0]>> 32) & 0xFFFFFFFF);
            if (!inputTypes.empty() && inputTypes[channelGateIndex].is_object()) {
                inputType = inputTypes[channelGateIndex];
            } else {
                THROW_LOGIC_EXCEPTION("Invalid channel input index.");
            }
        } else {
            inputType = inputTypes;
        }

        TypeInformation *typeInfo;

        if (inputType["kind"] == "basic")
        {
            std::string inputTypeName = inputType["type"];
            typeInfo = TypeInfoFactory::createTypeInfo(inputTypeName.c_str(), "TBD");
        } else if (inputType["kind"] == "Tuple") {
            typeInfo = TypeInfoFactory::createTupleTypeInfo(inputType["type"]);
        }
    //        typeInfo = TypeInfoFactory::createCommittableMessageInfo();
    //    }
        else
        {
            THROW_LOGIC_EXCEPTION("Unknown Input type" + to_string(inputType))
        }

        TypeSerializer *inputSerializer = typeInfo->createTypeSerializer("TBD");

        return StreamTaskNetworkInputFactory::create(inputSerializer, chanelInfos);
    }

    std::unique_ptr<StreamTaskNetworkOutput> StreamTask::createDataOutput(int operatorMethodIndicator)
    {
        // workaround, as for now streamflatmap operator is input
        LOG(">>> main operator address" + std::to_string(reinterpret_cast<long>(mainOperator_)))
        auto streamTaskNetworkOutput = std::make_unique<StreamTaskNetworkOutput>(mainOperator_, operatorMethodIndicator);
        return streamTaskNetworkOutput;
    }

    void StreamTask::resetOutputBufferAndRecordWriter()
    {
        // todo: need to check index 0
        if (recordWriter_->getRecordWriter(0) == nullptr) {
            return;
        }
        int32_t numberOfResult = recordWriter_->getRecordWriter(0)->getCounter();
        int32_t size = recordWriter_->getRecordWriter(0)->getLength();
        LOG("after  input_->emitNextProcessElement numberOfResult :" + std::to_string(numberOfResult) + " size " + std::to_string(size))

        LOG("after  input_->emitNextProcessElement  outputBufferStatus_ address:" + std::to_string(reinterpret_cast<long>(outputBufferStatus_))); //uintptr_t
        LOG("after  input_->emitNextProcessElement  outputBuffer addess:" + std::to_string(outputBufferStatus_->outputBuffer_)); //uintptr_t
        outputBufferStatus_->numberElement = numberOfResult;
        outputBufferStatus_->outputSize = size;

        // record writer clear for next call
        clearOutputBuffer();
    }

    void StreamTask::clearOutputBuffer()
    {
        recordWriter_->getRecordWriter(0)->clear();
    }

    OutputBufferStatus *StreamTask::getOutputBufferStatus()
    {
        return outputBufferStatus_;
    }

    StreamPartitioner<IOReadableWritable>* StreamTask::createPartitioner()
    {
        string partitionName = taskPartitionerConfig.getPartitionName();
        if (partitionName == "forward") {
            return new omnistream::datastream::ForwardPartitioner<IOReadableWritable>();
        } else if (partitionName == "rescale") {
            return new omnistream::datastream::RescalePartitioner<IOReadableWritable>();
        } else if (partitionName == "rebalance") {
            return new RebalancePartitioner<IOReadableWritable>();
        } else if (partitionName == "global") {
            return new GlobalPartitioner<IOReadableWritable>();
        } else if (partitionName == "hash") {
            return nullptr;
        } else {
            THROW_LOGIC_EXCEPTION("Partitioner not implemented");
        }
    }

    void StreamTask::addStreamOneInputProcessor(StreamOneInputProcessor *processor)
    {
        this->streamOneInputProcessors.push_back(processor);
    }

// Get a StreamOneInputProcessor from the vector by index
    StreamOneInputProcessor *StreamTask::getStreamOneInputProcessor(size_t index)
    {
        if (index >= streamOneInputProcessors.size())
        {
            throw std::out_of_range("Index out of range");
        }
        return streamOneInputProcessors[index];
    }

    StreamOneInputProcessor *StreamTask::createOmniInputProcessor(const json &inputChannelInfo, int operatorMethodIndicator)
    {
        if (inputChannelInfo.contains("input_channels"))
        {
            const json &channelInfos = inputChannelInfo["input_channels"];
            // Check if the array is present and valid

            if (channelInfos.is_array())
            {
                // Get the size of the array
                int array_size = channelInfos.size();

                // Create a C++ normal array (vector)
                std::vector<long> channel_array(array_size);

                // Copy elements from JSON array to C++ array
                for (int i = 0; i < array_size; ++i)
                {
                    channel_array[i] = channelInfos[i];
                }

                LOG(">>> createDataInput with numbe of channel" << array_size)
                return new StreamOneInputProcessor(createDataOutput(operatorMethodIndicator), createDataInput(channel_array));
            } else {
                THROW_LOGIC_EXCEPTION("No Input channel info provided in TDD.")
            }
        } else {
            THROW_LOGIC_EXCEPTION("No Input channel info provided in TDD.")
        }
    }

    std::shared_ptr<RecordWriterDelegate> StreamTask::createRecordWriterDelegate(const TaskInformationPOD& taskConfig)
    {
        std::vector<RecordWriter *> recordWriters = createRecordWriters(taskConfig);
        LOG("TaskInformation " << taskConfig.toString() << "and recorderWriter size " << recordWriters.size());
        if (recordWriters.size() == 1) {
            return std::make_shared<SingleRecordWriter>(recordWriters[0]);
        } else if (recordWriters.empty()) {
            return std::make_shared<NonRecordWriter>();
        } else {
            return std::make_shared<MultipleRecordWriters>(recordWriters);
        }
    }

    std::vector<RecordWriter *> StreamTask::createRecordWriters(const TaskInformationPOD& taskConfig)
    {
        auto recordWriters = std::vector<RecordWriter *>();
        auto outEdgesInOrder = taskConfig.getStreamConfigPOD().getOutEdgesInOrder();
        for (const auto & outEdge : outEdgesInOrder) {
            recordWriters.push_back(createRecordWriter(outEdge));
        }
        return recordWriters;
    }

    RecordWriter *StreamTask::createRecordWriter(const StreamEdgePOD& edge)
    {
        // output partitioner
        auto outputPartitioner = createPartitionerFromDesc(edge);
        int numberOfChannel = taskPartitionerConfig.getNumberOfChannel();
        outputPartitioner->setup(numberOfChannel);
        // workaround for now
        if (outputPartitioner == nullptr) {
            THROW_RUNTIME_ERROR("outputPartitioner is null");  // the below can not work
        }

        auto writer = new RecordWriter(outputBufferStatus_, outputPartitioner);
        return writer;
    }

// record write deleage

    StreamPartitioner<IOReadableWritable> * StreamTask::createPartitionerFromDesc(const StreamEdgePOD& edge)
    {
        const auto& partitioner = edge.getPartitioner();

        if (partitioner.getPartitionerName() == StreamPartitionerPOD::FORWARD) {
            return new ForwardPartitioner<IOReadableWritable>();
        } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::REBALANCE) {
            return new RebalancePartitioner<IOReadableWritable>();
        } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::RESCALE) {
            return new RescalePartitioner<IOReadableWritable>();
        } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::HASH) {
            int targetId = edge.getTargetId();
            int sourceId = edge.getSourceId();
            std::unordered_map<int, StreamConfigPOD> configMap = env_->taskConfiguration().getChainedConfigMap();
            auto description = configMap[sourceId].getOperatorDescription().getDescription();
            nlohmann::json config = nlohmann::json::parse(description);
            int32_t maxParallelism = env_->taskConfiguration().getMaxNumberOfSubtasks();
            return new KeyGroupStreamPartitioner<IOReadableWritable, Object>(config, targetId, maxParallelism);
        } else if (partitioner.getPartitionerName() == StreamPartitionerPOD::NONE) {
            throw std::invalid_argument("Invalid partitioner!");
        } else {
            throw std::invalid_argument("Invalid partitioner!");
        }
    }

    template<typename K>
    KeySelector<K>* StreamTask::buildKeySelector(std::vector<KeyFieldInfoPOD>& keyFields)
    {
        std::vector<int> keyCols;
        std::vector<int> keyTypes;
        for (auto & keyField : keyFields) {
            keyCols.emplace_back(keyField.getFieldIndex());
            if (keyField.getFieldTypeName().compare("BIGINT") == 0) {
                keyTypes.emplace_back(omniruntime::type::OMNI_LONG);
            }
            if (keyField.getFieldTypeName().compare("VARCHAR") == 0) {
                keyTypes.emplace_back(omniruntime::type::OMNI_VARCHAR);
            }
            if (keyField.getFieldTypeName().compare("INTEGER") == 0) {
                keyTypes.emplace_back(omniruntime::type::OMNI_INT);
            }
        }
        return new KeySelector<K>(keyTypes, keyCols);
    }
}