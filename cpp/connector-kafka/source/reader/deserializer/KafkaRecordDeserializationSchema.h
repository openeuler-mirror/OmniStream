/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_KAFKARECORDDESERIALIZATIONSCHEMA_H
#define FLINK_TNEL_KAFKARECORDDESERIALIZATIONSCHEMA_H

#include <memory>
#include <vector>
#include <map>
#include <stdexcept>
#include <iostream>
#include "connector-kafka/source/reader/RdKafkaConsumer.h"
#include "DynamicKafkaDeserializationSchema.h"
#include "core/api/common/serialization/DeserializationSchema.h"

class KafkaRecordDeserializationSchema {
public:
    virtual ~KafkaRecordDeserializationSchema() = default;

    virtual void open() = 0;

//    virtual void deserialize(std::vector<RdKafka::Message*> recordVec, Collector* out) = 0;
    virtual void deserialize(RdKafka::Message* recordVec, Collector* out) = 0;

    virtual void deserialize(std::vector<RdKafka::Message*> recordVec, Collector* out) = 0;

    static KafkaRecordDeserializationSchema* valueOnly(DeserializationSchema* valueDeserializationSchema);
};

#endif // FLINK_TNEL_KAFKARECORDDESERIALIZATIONSCHEMA_H
