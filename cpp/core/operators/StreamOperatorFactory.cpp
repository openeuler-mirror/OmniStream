/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <stdexcept>
#include <iostream>
#include <tasks/OmniStreamTask.h>
#include <utility>
#include "operatorconstants.h"
#include "StreamFlatMap.h"
#include "table/runtime/operators/TableOperatorConstants.h"
#include "StreamCalcBatch.h"
#include "StreamExpand.h"
#include "StreamSource.h"
#include "StreamMap.h"
#include "StreamFilter.h"
#include "StreamGroupedReduceOperator.h"
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "table/runtime/operators/join/StreamingJoinOperator.h"
#include "core/typeinfo/TypeInfoFactory.h"
#include "WatermarkAssignerOperator.h"
#include "table/runtime/operators/sink/SinkOperator.h"
#include "table/runtime/operators/sink/TimeStampInserterSinkOperator.h"
#include "streaming/api/operators/ProcessOperator.h"
#include "streaming/api/functions/sink/filesystem/StreamingFileWriter.h"
#include "streaming/api/functions/sink/filesystem/PartitionCommitter.h"
#include "runtime/operators/join/lookup/LookupJoinRunner.h"
#include "table/runtime/operators/source/InputFormatSourceFunction.h"
#include "table/runtime/operators/source/csv/CsvInputFormat.h"
#include "runtime/operators/window/LocalSlicingWindowAggOperator.h"
#include "core/task/ChainingOutput.h"
#include "datagen/nexmark/NexmarkSourceFunction.h"
#include "runtime/operators/sink/ConstraintEnforcer.h"
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/runtime/operators/window/processor/AbstractWindowAggProcessor.h"
#include "table/runtime/operators/rank/FastTop1Function.h"
#include "table/runtime/operators/join/window/InnerJoinOperator.h"
#include "runtime/operators/window/AggregateWindowOperator.h"
#include "datagen/meituan/JoinSource.h"
#include "datagen/meituan/MTFilterFunc.h"
#include "connector-kafka/source/KafkaSource.h"
#include "api/operators/SourceOperator.h"
#include "connector-kafka/sink/DeliveryGuarantee.h"
#include "connector-kafka/sink/KafkaSink.h"
#include "operators/sink/SinkWriterOperator.h"
#include "operators/sink/CommitterOperator.h"
#include "connector-kafka/sink/ProducerConfig.h"
#include "connector-kafka/utils/ConfigLoader.h"
#include "StreamOperatorFactory.h"

namespace omnistream {

    StreamOperator *StreamOperatorFactory::createOperatorAndCollector(omnistream::OperatorConfig &opConfig,
    WatermarkGaugeExposingOutput *chainOutput)
    {
        // NOT_IMPL_EXCEPTION
        auto uniqueName = opConfig.getUniqueName();
        LOG("getUniqueName :" + uniqueName)

    if (uniqueName == OPERATOR_NAME_STREAM_EXPAND) {
        auto *execExpand = new StreamExpand(opConfig.getDescription(), chainOutput);
        execExpand->setup();
        LOG("Operator StreamExpand address  " + std::to_string(reinterpret_cast<long>(execExpand)))
        return static_cast<OneInputStreamOperator *>(execExpand);
    } else if (uniqueName == OPERATOR_NAME_STREAM_CALC) {
        auto *execCalc = new StreamCalcBatch(opConfig.getDescription(), chainOutput);
        execCalc->setup();
        LOG("Operator StreamCalcBatch address  " + std::to_string(reinterpret_cast<long>(execCalc)))
        return static_cast<OneInputStreamOperator *>(execCalc);
    } else if (uniqueName == OPERATOR_NAME_STREAM_JOIN) {
        // todo this ios test
        LOG("Generating StreamingJoinOperator...")
        auto *op = new StreamingJoinOperator<RowData*>(opConfig.getDescription(), chainOutput);
        op->setup();
        return static_cast<AbstractTwoInputStreamOperator *>(op);
    } else if (uniqueName == OPERATOR_NAME_WATERMARK_ASSIGNER) {
        auto *watermarkAssignerOperator = new WatermarkAssignerOperator(chainOutput,
            opConfig.getDescription()["rowtimeFieldIndex"],
            4000,
            opConfig.getDescription()["idleTimeout"]);
        watermarkAssignerOperator->setup();
        return static_cast<OneInputStreamOperator *>(watermarkAssignerOperator);
    } else if (uniqueName == OPERATOR_NAME_KEYED_PROCESS_OPERATOR) {
        if (opConfig.getName()[0] == 'D') {
            RowTimeDeduplicateFunction *func = new RowTimeDeduplicateFunction(opConfig.getDescription());
            auto *op = new KeyedProcessOperator(func, chainOutput, opConfig.getDescription());
            op->setup();
            LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)))

                return static_cast<OneInputStreamOperator *>(op);
            } else {
                GroupAggFunction *func = new GroupAggFunction(0l, opConfig.getDescription());
                auto *op = new KeyedProcessOperator(func, chainOutput, opConfig.getDescription());
                op->setup();
                LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)))

                return static_cast<OneInputStreamOperator *>(op);
            }
        } else if (uniqueName == OPERATOR_NAME_SINK ||
                   uniqueName == OPERATOR_NAME_COLLECT_SINK) {
            nlohmann::json object;
            if (opConfig.getName() != "") {
                object["outputfile"] = "/tmp/" + opConfig.getName() + ".txt";
            } else {
                object["outputfile"] = "/tmp/sink.txt";
            }

            return static_cast<OneInputStreamOperator *>(new SinkOperator(object));
        } else if (uniqueName == OPERATOR_NAME_PROCESS_OPERATOR) {
            LookupJoinRunner *runner = new LookupJoinRunner(opConfig.getDescription(), chainOutput);
            ProcessOperator *op = new ProcessOperator(runner, opConfig.getDescription(), chainOutput);
            op->setup();
            LOG("Operator ProcessOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return static_cast<OneInputStreamOperator *>(op);
        } else if (uniqueName == OPERATOR_NAME_LOCAL_WINDOW_AGG) {
            auto *op = new LocalSlicingWindowAggOperator(opConfig.getDescription(), chainOutput);
            op->setup();
            LOG("Operator LocalSlicingWindowAggOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return static_cast<OneInputStreamOperator *>(op);
        } else if (uniqueName == OPERATOR_NAME_GLOBAL_WINDOW_AGG) {
            auto *processor = new AbstractWindowAggProcessor(opConfig.getDescription(), chainOutput);
            auto *op = new SlicingWindowOperator<RowData *, int64_t>(processor, opConfig.getDescription());
            op->setup();
            LOG("Operator SlicingWindowOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return static_cast<OneInputStreamOperator *>(op);
        } else if (opConfig.getUniqueName() == OPERATOR_NAME_GROUP_WINDOW_AGG) {
            auto *op = new AggregateWindowOperator<RowData*, TimeWindow>(opConfig.getDescription(), chainOutput);
            op->setup();
            LOG("Operator AggregateWindowOperator address " + std::to_string(reinterpret_cast<long>(op)))
            return static_cast<OneInputStreamOperator *>(op);
        } else if (uniqueName == OPERATOR_NAME_WINDOW_INNER_JOIN) {
            auto op = new InnerJoinOperator<int64_t>(opConfig.getDescription(), chainOutput, nullptr, nullptr);
            op->setup();
            LOG("Operator WindowJoinOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return static_cast<AbstractTwoInputStreamOperator *>(op);
        } else {
            THROW_LOGIC_EXCEPTION("Unknown operator " + uniqueName);
        }
    }
/**
 * std::shared_ptr<OmniStreamTask> task is ressareve for futur.
 **/

    StreamOperator *StreamOperatorFactory::createOperatorAndCollector(omnistream::OperatorPOD &opDesc,
    WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<OmniStreamTask> task)
    {
        auto operatorID = opDesc.getId();
        LOG("getID  :" << operatorID)

        if (operatorID == OPERATOR_NAME_STREAM_CALC) {
            return CreateStreamCalcOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_STREAM_JOIN) {
            return CreateStreamJoinOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_LOCAL_WINDOW_AGG) {
            return CreateLocalWindowAggOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_GLOBAL_WINDOW_AGG) {
            return CreateGlobalWindowAggOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_GROUP_WINDOW_AGG) {
            return CreateGroupAggOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_WATERMARK_ASSIGNER) {
            // Idle timeout is currently set to 0, but might change depending on the config
            return CreateWatermarkAssignerOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_KEYED_PROCESS_OPERATOR) {
            return CreateKeyedProcessOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_SINK || operatorID == OPERATOR_NAME_COLLECT_SINK
                    || operatorID == OPERATOR_NAME_STREAM_SINK) {
            return CreateSinkOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_STREAM_SOURCE || operatorID == OPERATOR_NAME_SOURCE_OPERATOR) {
            return CreateSourceOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_STREAM_EXPAND) {
            return CreateStreamExpandOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_STREAMRECORDTIMESTAMPINSERTER) {
            return CreateTimestampInserterOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_PROCESS_OPERATOR) {
            return CreateProcessOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_CONSTRAINTENFORCER) {
            return CreateConstraintEnforcerOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_WINDOW_INNER_JOIN) {
            return CreateWindowInnerJoinOp(opDesc, chainOutput, task);
        } else if (operatorID == datastream::OPERATOR_NAME_FLATMAP) {
            return CreateFlatMapOp(opDesc, chainOutput);
        } else if (operatorID == datastream::OPERATOR_NAME_MAP) {
            return CreateMapOp(opDesc, chainOutput);
        } else if (operatorID == datastream::OPERATOR_NAME_REDUCE) {
            return CreateReduceOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_FILTER) {
            return CreateFilterOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_SINK_WRITER) {
            return CreateSinkWriterOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_COMMIT_OPERATOR) {
            auto description = opDesc.getDescription();
            nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
            auto committerOperator = new CommitterOperator(opDescriptionJSON["batch"]);
            return static_cast<OneInputStreamOperator *>(committerOperator);
        } else if (operatorID == OPERATOR_NAME_STREAMING_FILE_WRITER) {
            return CreateStreamingFileWriterOp(opDesc, chainOutput, task);
        } else if (operatorID == OPERATOR_NAME_PARTITION_COMMITTER) {
            return CreatePartitionCommitterOp(opDesc, chainOutput, task);
        } else {
            return nullptr;
        }
    }

    StreamOperator* StreamOperatorFactory::CreateStreamCalcOp(OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *execCalc = new StreamCalcBatch(opDescriptionJSON, chainOutput);
        execCalc->setup(std::move(task));

        LOG("Operator StreamCalc address  " + std::to_string(reinterpret_cast<long>(execCalc)))
        return static_cast<OneInputStreamOperator *>(execCalc);
    }

    StreamOperator* StreamOperatorFactory::CreateStreamJoinOp(OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        LOG("Generating StreamingJoinOperator...")
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *op = new StreamingJoinOperator<RowData*>(opDescriptionJSON, chainOutput);
        op->setup(std::move(task));
        return static_cast<AbstractTwoInputStreamOperator *>(op);
    }

    StreamOperator* StreamOperatorFactory::CreateLocalWindowAggOp(OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *op = new LocalSlicingWindowAggOperator(opDescriptionJSON, chainOutput);
        op->setup(std::move(task));
        LOG("Operator LocalSlicingWindowAggOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator *>(op);
    }

    StreamOperator* StreamOperatorFactory::CreateGlobalWindowAggOp(OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *processor = new AbstractWindowAggProcessor(opDescriptionJSON, chainOutput);
        auto *op = new SlicingWindowOperator<RowData *, int64_t>(processor, opDescriptionJSON);
        op->setup(std::move(task));
        LOG("Operator SlicingWindowOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator *>(op);
    }

    StreamOperator* StreamOperatorFactory::CreateGroupAggOp(OperatorPOD &opConfig,
                                                            WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *op = new AggregateWindowOperator<RowData*, TimeWindow>(opDescriptionJSON, chainOutput);
        op->setup(std::move(task));
        LOG("Operator AggregateWindowOperator address " + std::to_string(reinterpret_cast<long>(op)))
        return static_cast<OneInputStreamOperator *>(op);
    }

    StreamOperator* StreamOperatorFactory::CreateWatermarkAssignerOp(OperatorPOD &opConfig,
                                                                     WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *watermarkAssignerOperator = new WatermarkAssignerOperator(chainOutput, opDescriptionJSON["rowtimeFieldIndex"],
                                                                        opDescriptionJSON["intervalSecond"], 0);
        return static_cast<OneInputStreamOperator *>(watermarkAssignerOperator);
    }

    StreamOperator* StreamOperatorFactory::CreateKeyedProcessOp(OperatorPOD &opConfig,
                                                                WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        LOG("opDesc.getName: " << opConfig.getName())
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);

        if (opConfig.getName()[0] == 'D') {
            LOG("Deduplicate description: " << opConfig.getDescription())
            RowTimeDeduplicateFunction *func = new RowTimeDeduplicateFunction(opDescriptionJSON);
            LOG("Deduplicate2")
            auto *op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
            LOG("Deduplicate3")
            op->setup(std::move(task));
            LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)))

            return static_cast<OneInputStreamOperator *>(op);
        } else if (opDescriptionJSON.contains("processFunction")
                   && opDescriptionJSON["processFunction"] == "AppendOnlyTopNFunction") {
            LOG("AppendOnlyTopNFunction description: " << opConfig.getDescription())
            AbstractTopNFunction<RowData*> *func = new AppendOnlyTopNFunction<RowData*>(opDescriptionJSON);
            auto *op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
            op->setup(std::move(task));
            op->setDescription(opDescriptionJSON);
            LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)))
            return static_cast<OneInputStreamOperator *>(op);
        } else if (opDescriptionJSON.contains("processFunction")
                   && opDescriptionJSON["processFunction"] == "FastTop1Function") {
            AbstractTopNFunction<RowData*> *func = new FastTop1Function<RowData*>(opDescriptionJSON);
            auto *op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
            op->setup(std::move(task));
            op->setDescription(opDescriptionJSON);
            LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)))
            return static_cast<OneInputStreamOperator *>(op);
        } else {
            GroupAggFunction *func = new GroupAggFunction(0l, opDescriptionJSON);
            auto *op = new KeyedProcessOperator(func, chainOutput, opDescriptionJSON);
            op->setup(std::move(task));
            op->setDescription(opDescriptionJSON);
            LOG("Operator KeyedProcessOperator address  " + std::to_string(reinterpret_cast<long>(op)))

            return static_cast<OneInputStreamOperator *>(op);
        }
        return nullptr;
    }

StreamOperator* StreamOperatorFactory::CreateSinkOp(OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput,
    std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    const char* env = std::getenv("WRITE_TO_FILE");
    opDescriptionJSON["outputfile"] = (env && std::string(env) == "TRUE") ? std::string("/tmp/flink_output.txt") : "";
    return static_cast<OneInputStreamOperator *>(new SinkOperator(opDescriptionJSON));
}

    StreamOperator* StreamOperatorFactory::CreateSourceOp(OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        if (opDescriptionJSON["format"] == "csv") {
            std::vector<DataTypeId> fields;
            for (auto &field : opDescriptionJSON["fields"]) {
                fields.push_back(LogicalType::flinkTypeToOmniTypeId(field["type"]));
            }
            omnistream::csv::CsvSchema schema(fields);

            std::vector<int> csvSelectFieldToProjectFieldMapping =
                    opDescriptionJSON["csvSelectFieldToProjectFieldMapping"];
            std::vector<int> csvSelectFieldToCsvFieldMapping = opDescriptionJSON["csvSelectFieldToCsvFieldMapping"];
            std::vector<int> oneMap;
            oneMap.resize(csvSelectFieldToProjectFieldMapping.size());
            for (size_t i = 0; i < csvSelectFieldToProjectFieldMapping.size(); i++) {
                oneMap[csvSelectFieldToProjectFieldMapping[i]] = csvSelectFieldToCsvFieldMapping[i];
            }
            // use small batch size for testing
            constexpr int batchSize = 3;
            auto csvInputFormat = new omnistream::csv::CsvInputFormat<omnistream::VectorBatch>(schema, batchSize, oneMap);
            constexpr int fileLength = 100000;
            InputSplit *inputSplit = new InputSplit(opDescriptionJSON["filePath"], 0, fileLength);

            auto *func = new InputFormatSourceFunction<omnistream::VectorBatch>(csvInputFormat, inputSplit);

            auto *source = new StreamSource<omnistream::VectorBatch>(func, chainOutput);
            source->setup(std::move(task));
            return static_cast<StreamOperator *>(source);
        } else if (opDescriptionJSON["format"] == "nexmark") {
            int batchSize = opDescriptionJSON["batchSize"];
            BatchEventDeserializer *eventDeserializer = new BatchEventDeserializer(batchSize);
            auto typeInfo = TypeInfoFactory::createTypeInfo("String", "TBD");
            // In NexmarkConfiguration, all values have been set to their default value
            NexmarkConfiguration nexmarkConfig(opDescriptionJSON);
            GeneratorConfig config{nexmarkConfig};

            auto *func = new NexmarkSourceFunction<omnistream::VectorBatch>(config, eventDeserializer, typeInfo);
            auto *source = new StreamSource<omnistream::VectorBatch>(func, chainOutput);
            source->setup(std::move(task));
            source->setDescription(opDescriptionJSON);
            return static_cast<StreamOperator *>(source);
        } else if (opDescriptionJSON["format"] == "joinSource") {
            // In JoinSource, all values have been set to their default value
            JoinSource *func = new JoinSource(opDescriptionJSON);
            auto *source = new StreamSource<omnistream::VectorBatch>(func, chainOutput);
            source->setup(std::move(task));
            source->setDescription(opDescriptionJSON);
            return static_cast<StreamOperator *>(source);
        } else if (opDescriptionJSON["format"] == "kafka") {
            auto description = opConfig.getDescription();
            nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
            // create kafka source
            std::shared_ptr<KafkaSource> source = std::make_shared<KafkaSource>(opDescriptionJSON);
            ProcessingTimeService* timeService = new SystemProcessingTimeService();
            auto *op = new SourceOperator(chainOutput, opDescriptionJSON, source, timeService);
            op->setup(std::move(task));
            LOG("Operator SourceOperator address " + std::to_string(reinterpret_cast<long>(op)));
            return op;
        } else {
            auto *source = new omnistream::StreamSource<Object>(chainOutput, opDescriptionJSON);
            source->setup(std::move(task));
            return static_cast<StreamOperator *>(source);
        }
    }

    StreamOperator* StreamOperatorFactory::CreateStreamExpandOp(OperatorPOD &opConfig,
                                                                WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *execExpand = new StreamExpand(opDescriptionJSON, chainOutput);
        execExpand->setup(std::move(task));
        LOG("Operator StreamExpand address  " + std::to_string(reinterpret_cast<long>(execExpand)))
        return static_cast<OneInputStreamOperator *>(execExpand);
    }

    StreamOperator* StreamOperatorFactory::CreateTimestampInserterOp(OperatorPOD &opConfig,
                                                                     WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        LOG("create StreamRecordTimestampInserterOperator")
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        nlohmann::json object;
        object["outputfile"] = "/tmp/flink_output.txt";
        return static_cast<OneInputStreamOperator *>(
                new TimeStampInserterSinkOperator(object, chainOutput, opDescriptionJSON));
    }

    StreamOperator *StreamOperatorFactory::CreateProcessOp(OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        // Currently it is only for lookupjoin
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        LookupJoinRunner *runner = new LookupJoinRunner(opDescriptionJSON, chainOutput);
        ProcessOperator *op = new ProcessOperator(runner, opDescriptionJSON, chainOutput);
        op->setup(std::move(task));
        LOG("Operator ProcessOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator *>(op);
    }

    StreamOperator *StreamOperatorFactory::CreateConstraintEnforcerOp(OperatorPOD &opConfig,
                                                                      WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto *op = new ConstraintEnforcer(chainOutput);
        LOG("Operator ConstraintEnforcer address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<OneInputStreamOperator *>(op);
    }

    StreamOperator* StreamOperatorFactory::CreateWindowInnerJoinOp(OperatorPOD &opConfig,
                                                                   WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto op = new InnerJoinOperator<int64_t>(opDescriptionJSON, chainOutput, nullptr, nullptr);
        op->setup(std::move(task));
        LOG("Operator WindowJoinOperator address " + std::to_string(reinterpret_cast<long>(op)));
        return static_cast<AbstractTwoInputStreamOperator *>(op);
    }

    StreamOperator *StreamOperatorFactory::CreateMapOp(omnistream::OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *mapOp = new datastream::StreamMap<Object, Object *>(chainOutput, opDescriptionJSON);
        return static_cast<OneInputStreamOperator *>(mapOp);
    }

    StreamOperator *StreamOperatorFactory::CreateFilterOp(omnistream::OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *filterOp = new omnistream::datastream::StreamFilter<Object, Object *>(chainOutput, opDescriptionJSON);
        return static_cast<OneInputStreamOperator *>(filterOp);
    }

    StreamOperator *StreamOperatorFactory::CreateFlatMapOp(omnistream::OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput)
    {
        auto description = opConfig.getDescription();
        nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
        auto *op = new datastream::StreamFlatMap<Object, Object *>(chainOutput, opDescriptionJSON);
        return static_cast<OneInputStreamOperator *>(op);
    }

StreamOperator *StreamOperatorFactory::CreateStreamingFileWriterOp(OperatorPOD &opConfig,
    WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);

    auto* bucketsBuilder = new BulkFormatBuilder<omnistream::VectorBatch*, std::string>(opDescriptionJSON);
    int bucketCheckInterval = 1000;

    auto *op = new StreamingFileWriter<omnistream::VectorBatch*>(
        bucketCheckInterval,
        bucketsBuilder
    );
    op->setOutput(chainOutput);
    op->setup();

    LOG("Operator StreamingFileWriter address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator *>(op);
}

StreamOperator *StreamOperatorFactory::CreatePartitionCommitterOp(OperatorPOD &opConfig,
    WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto *op = new PartitionCommitter();

    LOG("Operator PartitionCommitter address " + std::to_string(reinterpret_cast<long>(op)));
    return static_cast<OneInputStreamOperator *>(op);
}

StreamOperator *StreamOperatorFactory::CreateReduceOp(omnistream::OperatorPOD &opConfig, WatermarkGaugeExposingOutput *chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    auto *op = new datastream::StreamGroupedReduceOperator<Object>(chainOutput, opDescriptionJSON);
    op->setup();
    return static_cast<OneInputStreamOperator *>(op);
}

StreamOperator* StreamOperatorFactory::CreateSinkWriterOp(omnistream::OperatorPOD &opConfig,
    WatermarkGaugeExposingOutput* chainOutput, std::shared_ptr<omnistream::OmniStreamTask> task)
{
    DeliveryGuarantee deliveryGuarantee;
    auto description = opConfig.getDescription();
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    std::string deliveryGuaranteeStr = opDescriptionJSON["deliveryGuarantee"];
    LOG("deliveryGuaranteeStr:" + deliveryGuaranteeStr)
    if (deliveryGuaranteeStr == "EXACTLY_ONCE") {
        deliveryGuarantee = DeliveryGuarantee::EXACTLY_ONCE;
    } else if (deliveryGuaranteeStr == "AT_LEAST_ONCE") {
        deliveryGuarantee = DeliveryGuarantee::AT_LEAST_ONCE;
    } else {
        deliveryGuarantee = DeliveryGuarantee::NONE;
    }
    auto kafkaProducerConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    nlohmann::json kafkaProducerConfigJson = opDescriptionJSON["kafkaProducerConfig"];
    int64_t maxPushRecords = 100000;
    for (const auto& element : kafkaProducerConfigJson.items()) {
        std::string key = element.key();
        std::string value = element.value();
        std::string errorString;

        // check kv
        const std::map<std::string, std::string>::iterator &iter = ProducerConfig.find(key);
        if (iter != ProducerConfig.end() && iter->second != "") {
            kafkaProducerConfig->set(iter->second, value, errorString);
        }

        // opt kafka producer para from file
        std::unordered_map<std::string, std::string> optConsumerConfig = ConfigLoader::GetKafkaProducerConfig();
        for (const auto &pair : optConsumerConfig) {
            if (pair.first == "max.push.records") {
                maxPushRecords = std::stol(pair.second);
            } else {
                kafkaProducerConfig->set(pair.first, pair.second, errorString);
            }
        }

        LOG("kafkaProducerConfig set key:" << key << " value:" << value)
    }
    std::string transactionalIdPrefix = opDescriptionJSON["transactionalIdPrefix"];
    LOG("transactionalIdPrefix:" + transactionalIdPrefix)
    std::string topic = opDescriptionJSON["topic"];
    LOG("topic:" + topic)
    auto kafkaSink = new KafkaSink(deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix,
        topic, opDescriptionJSON, maxPushRecords);
    return static_cast<OneInputStreamOperator *>(new SinkWriterOperator(kafkaSink, opDescriptionJSON));
}
}