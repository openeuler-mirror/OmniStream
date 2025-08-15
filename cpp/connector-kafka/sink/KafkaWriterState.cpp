/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "KafkaWriterState.h"
#include <stdexcept>
#include <functional>

KafkaWriterState::KafkaWriterState(const std::string& transactionalIdPrefix)
{
    if (transactionalIdPrefix.empty()) {
        throw std::invalid_argument("transactionalIdPrefix");
    }
    this->transactionalIdPrefix = transactionalIdPrefix;
}

std::string KafkaWriterState::getTransactionalIdPrefix() const
{
    return transactionalIdPrefix;
}

bool KafkaWriterState::operator==(const KafkaWriterState& that) const
{
    return this == &that || transactionalIdPrefix == that.transactionalIdPrefix;
}

bool KafkaWriterState::operator!=(const KafkaWriterState& that) const
{
    return !(*this == that);
}

size_t KafkaWriterState::hashCode() const
{
    return std::hash<std::string>{}(transactionalIdPrefix);
}

std::string KafkaWriterState::toString() const
{
    return "KafkaWriterState{, transactionalIdPrefix='" + transactionalIdPrefix + "'}";
}
