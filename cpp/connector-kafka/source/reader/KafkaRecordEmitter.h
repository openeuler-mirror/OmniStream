/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_KAFKARECORDEMITTER_H
#define FLINK_TNEL_KAFKARECORDEMITTER_H

#include <vector>
#include <string_view>
#include "../split/KafkaPartitionSplitState.h"
#include "functions/Collector.h"
#include "RecordEmitter.h"
#include "connector-kafka/source/reader/deserializer/KafkaRecordDeserializationSchema.h"
#include "table/vectorbatch/VectorBatch.h"


class KafkaRecordEmitter : public RecordEmitter<RdKafka::Message, KafkaPartitionSplitState> {
public:
    ~KafkaRecordEmitter() override;

    explicit KafkaRecordEmitter(KafkaRecordDeserializationSchema* deserializationSchema);

    void emitRecord(RdKafka::Message* consumerRecord, SourceOutput* output,
        std::shared_ptr<KafkaPartitionSplitState>& splitState) override;

    void emitBatchRecord(const std::vector<RdKafka::Message*>& messageVec, SourceOutput* output,
        std::shared_ptr<KafkaPartitionSplitState>& splitState) override;
private:
    class SourceOutputWrapper : public Collector {
    public:
        void collect(void* record) override
        {
            curSourceOutput->Collect(record, curTimestamp);
        }

        void close() override {}

        void setSourceOutput(SourceOutput* sourceOutput)
        {
            curSourceOutput = sourceOutput;
        }

        void setTimestamp(long timestamp)
        {
            this->curTimestamp = timestamp;
        }
    private:
        SourceOutput* curSourceOutput;
        long curTimestamp;
    };

    KafkaRecordDeserializationSchema* deserializationSchema;
    SourceOutputWrapper* sourceOutputWrapper;
};

#endif // FLINK_TNEL_KAFKARECORDEMITTER_H
