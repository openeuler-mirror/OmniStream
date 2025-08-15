/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef OMNIFLINK_KAFKASINK_H
#define OMNIFLINK_KAFKASINK_H

#include "KafkaWriterState.h"
#include "KafkaCommittable.h"
#include "KafkaRecordSerializationSchema.h"
#include "KafkaWriter.h"

class KafkaSink {
public:
    KafkaSink(DeliveryGuarantee deliveryGuarantee,
              RdKafka::Conf* kafkaProducerConfig,
              std::string& transactionalIdPrefix,
              std::string& topic,
              const nlohmann::json& opDescriptionJSON,
              int64_t maxPushRecords);

    KafkaCommittable *CreateCommitter();
    KafkaWriter *CreateWriter();

private:
    DeliveryGuarantee deliveryGuarantee;
    RdKafka::Conf* kafkaProducerConfig;
    std::string transactionalIdPrefix;
    std::string topic;
    nlohmann::json description;
    int64_t maxPushRecords;
};

#endif // OMNIFLINK_KAFKASINK_H
