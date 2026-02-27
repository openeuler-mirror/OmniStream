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
    KafkaPartitionSplitState* splitState)
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
    KafkaPartitionSplitState* splitState)
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