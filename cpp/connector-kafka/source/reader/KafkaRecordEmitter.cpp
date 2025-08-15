/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "KafkaRecordEmitter.h"
#include <stdexcept>
#include "datagen/meituan/OriginalRecord.h"

KafkaRecordEmitter::KafkaRecordEmitter(KafkaRecordDeserializationSchema* deserializationSchema)
    : deserializationSchema(deserializationSchema), sourceOutputWrapper(new SourceOutputWrapper())
{
}

KafkaRecordEmitter::~KafkaRecordEmitter()
{
    delete deserializationSchema;
    delete sourceOutputWrapper;
}

void KafkaRecordEmitter::emitRecord(RdKafka::Message* consumerRecord, SourceOutput* output,
    std::shared_ptr<KafkaPartitionSplitState>& splitState)
{
    try {
        sourceOutputWrapper->setSourceOutput(output);
        sourceOutputWrapper->setTimestamp(consumerRecord->timestamp().timestamp);
        deserializationSchema->deserialize(consumerRecord, sourceOutputWrapper);
        splitState->setCurrentOffset(consumerRecord->offset() + 1);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to deserialize consumer record due to: " + std::string(e.what()));
    }
}

void KafkaRecordEmitter::emitBatchRecord(
    const std::vector<RdKafka::Message*>& messageVec, SourceOutput* output,
    std::shared_ptr<KafkaPartitionSplitState>& splitState)
{
    try {
        sourceOutputWrapper->setSourceOutput(output);
        sourceOutputWrapper->setTimestamp(messageVec.back()->timestamp().timestamp);
        deserializationSchema->deserialize(messageVec, sourceOutputWrapper);
        splitState->setCurrentOffset(messageVec.back()->offset() + 1);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to deserialize consumer record due to: " + std::string(e.what()));
    }
}