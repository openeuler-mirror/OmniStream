/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef OMNIFLINK_SINKWRITEROPERATOR_H
#define OMNIFLINK_SINKWRITEROPERATOR_H

#include <vector>
#include <memory>
#include <optional>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <limits>
#include "core/include/common.h"
#include "connector/kafka/sink/KafkaSink.h"
#include "connector/kafka/sink/KafkaWriter.h"
#include "connector/kafka/sink/KafkaCommittable.h"
#include "connector/kafka/sink/KafkaCommittableSerializer.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"
#include "streaming/api/operators/sink/KafkaSinkWriterStateHandler.h"
#include "streaming/api/operators/sink/InitContextImpl.h"

// 静态常量定义
static std::string streamingCommitterRawStatesName = "streaming_committer_raw_states";

class SinkWriterOperator : public OneInputStreamOperator, public AbstractStreamOperator<void *> {
public:
    static ListStateDescriptor<std::vector<uint8_t>> STREAMING_COMMITTER_RAW_STATES_DESC;

    SinkWriterOperator(KafkaSink *kafkaSink, const nlohmann::json& config);

    ~SinkWriterOperator()
    {
        EndInput();
        delete kafkaSink;
        delete sinkWriter;
        delete writerStateHandler;
    }

    void initializeState(StateInitializationContextImpl<void*>* context) override;

    void snapshotState(StateSnapshotContextSynchronousImpl* context) override;

    void open() override;

    RowData* getOutputEntireRow(omnistream::VectorBatch *batch, int rowId);

    void processBatch(StreamRecord *record) override;

    void processElement(StreamRecord *record) override;

    void EndInput();

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override;

    void ProcessWatermark(Watermark *watermark) override;

    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override;

    bool canBeStreamOperator() override
    {
        return isDataStream;
    }

    std::string getTypeName() override;

private:
    template<typename K>
    InitContextImpl<K>* createInitContext(std::optional<uint64_t> restoredCheckpointId);
    template<typename CommT>
    void emitCommittables(std::int64_t checkpointId);
    template<typename CommT>
    void emit(int indexOfThisSubtask, int numberOfParallelSubtasks, std::int64_t checkpointId,
              const std::vector<CommT> &committables);
    KafkaCommittableSerializer *committableSerializer{};
    bool emitDownstream{};
    std::int64_t currentWatermark{};
    KafkaSink *kafkaSink;
    KafkaWriter *sinkWriter;
    bool endOfInput;
    nlohmann::json description;
    std::vector<std::string> inputTypes;

    ProcessingTimeServiceImpl* processingTimeService;
    KafkaSinkWriterStateHandler* writerStateHandler;
    std::vector<KafkaCommittable> legacyCommittables;
    bool isDataStream;
    int32_t subtaskIndex;
};
#endif // OMNIFLINK_SINKWRITEROPERATOR_H
