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

#ifndef OMNIFLINK_KAFKACOMMITTER_H
#define OMNIFLINK_KAFKACOMMITTER_H

#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include "FlinkKafkaInternalProducer.h"
#include "KafkaCommittable.h"
#include "Recyclable.h"
#include "streaming/api/operators/sink/committables/Committer.h"

template <typename CommT>
class KafkaCommitter : public Committer<CommT> {
public:
    explicit KafkaCommitter(const RdKafka::Conf* kafkaProducerConfig);
    void Commit(std::vector<CommitRequest<KafkaCommittable>>& requests);
    void Close();
private:
    RdKafka::Conf* kafkaProducerConfig;
    std::shared_ptr<FlinkKafkaInternalProducer> recoveryProducer;
};

#endif // OMNIFLINK_KAFKACOMMITTER_H
