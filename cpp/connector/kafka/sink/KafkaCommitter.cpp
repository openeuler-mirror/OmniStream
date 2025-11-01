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

#include "KafkaCommitter.h"

template <typename CommT>
KafkaCommitter<CommT>::KafkaCommitter(const RdKafka::Conf* kafkaProducerConfig)
    : kafkaProducerConfig(const_cast<RdKafka::Conf*>(kafkaProducerConfig)), recoveryProducer(nullptr) {}

template <typename CommT>
void KafkaCommitter<CommT>::Commit(std::vector<CommitRequest<KafkaCommittable>>& requests)
{
    for (auto& request : requests) {
        const KafkaCommittable& committable = request.GetCommittable();
        const std::string& transactionalId = committable.GetTransactionalId();
        auto recyclable = committable.GetProducer().value().get();
        FlinkKafkaInternalProducer *producer = recyclable->GetObject();
        try {
            producer->CommitTransaction();
            producer->Flush();
            if (recyclable) {
                recyclable->Close();
            }
        } catch (const std::exception& e) {
            if (recyclable) {
                recyclable->Close();
            }
            request.signalFailedWithUnknownReason(e);
        }
    }
}

template <typename CommT>
void KafkaCommitter<CommT>::Close()
{
    if (recoveryProducer) {
        recoveryProducer->Close();
    }
}