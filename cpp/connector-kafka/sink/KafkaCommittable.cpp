/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "KafkaCommittable.h"

KafkaCommittable::KafkaCommittable(long producerId,
                                   short epoch,
                                   const std::string& transactionalId,
                                   std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>> producer)
    : producerId(producerId),
    epoch(epoch),
    transactionalId(transactionalId),
    producer(producer) {}

KafkaCommittable KafkaCommittable::of(FlinkKafkaInternalProducer* producer,
                                      std::function<void(FlinkKafkaInternalProducer*)> recycler)
{
    return KafkaCommittable(producer->getProducerId(),
        producer->getEpoch(),
        producer->getTransactionalId(),
        std::make_shared<Recyclable<FlinkKafkaInternalProducer>>(producer, recycler));
}

long KafkaCommittable::GetProducerId() const
{
    return producerId;
}

short KafkaCommittable::GetEpoch() const
{
    return epoch;
}

const std::string& KafkaCommittable::GetTransactionalId() const
{
    return transactionalId;
}

std::optional<std::shared_ptr<Recyclable<FlinkKafkaInternalProducer>>> KafkaCommittable::GetProducer() const
{
    return producer;
}

std::string KafkaCommittable::toString() const
{
    return std::string("KafkaCommittable{")
           + "producerId=" + std::to_string(producerId)
           + ", epoch=" + std::to_string(epoch)
           + ", transactionalId=" + transactionalId
           + '}';
}

bool KafkaCommittable::operator==(const KafkaCommittable& that) const
{
    return producerId == that.producerId
           && epoch == that.epoch
           && transactionalId == that.transactionalId;
}

bool KafkaCommittable::operator!=(const KafkaCommittable& that) const
{
    return !(*this == that);
}