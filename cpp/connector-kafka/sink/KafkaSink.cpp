/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "KafkaSink.h"

KafkaSink::KafkaSink(DeliveryGuarantee deliveryGuarantee,
                     RdKafka::Conf* kafkaProducerConfig,
                     std::string& transactionalIdPrefix,
                     std::string& topic,
                     const nlohmann::json& description,
                     int64_t maxPushRecords)
    : deliveryGuarantee(deliveryGuarantee),
    kafkaProducerConfig(kafkaProducerConfig),
    transactionalIdPrefix(transactionalIdPrefix),
    topic(topic),
    description(description),
    maxPushRecords(maxPushRecords) {}


KafkaCommittable *KafkaSink::CreateCommitter() {
    return nullptr;
}


KafkaWriter *KafkaSink::CreateWriter()
{
    return new KafkaWriter(deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix, topic, description,
                           maxPushRecords);
}
