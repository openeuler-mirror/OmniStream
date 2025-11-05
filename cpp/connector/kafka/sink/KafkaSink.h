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
