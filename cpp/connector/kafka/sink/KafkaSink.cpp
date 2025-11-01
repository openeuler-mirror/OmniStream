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


KafkaCommittable *KafkaSink::CreateCommitter()
{
    return nullptr;
}


KafkaWriter *KafkaSink::CreateWriter()
{
    return new KafkaWriter(deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix, topic, description,
                           maxPushRecords);
}
