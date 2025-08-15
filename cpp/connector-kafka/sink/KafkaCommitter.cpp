/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
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