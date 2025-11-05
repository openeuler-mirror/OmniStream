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
#include "connector/kafka/sink/KafkaCommittableSerializer.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/OneInputStreamOperator.h"
#include "streaming/api/operators/AbstractStreamOperator.h"

class SinkWriterOperator : public OneInputStreamOperator, AbstractStreamOperator<void *> {
public:

    SinkWriterOperator(KafkaSink *kafkaSink, const nlohmann::json& config);

    ~SinkWriterOperator()
    {
        EndInput();
        delete kafkaSink;
        delete sinkWriter;
    }

    void initializeState();

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

private:
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
    bool isDataStream;
    int32_t subtaskIndex;
};
#endif // OMNIFLINK_SINKWRITEROPERATOR_H
