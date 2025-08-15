/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "KafkaWriterStateSerializer.h"
#include <sstream>

int KafkaWriterStateSerializer::GetVersion() const
{
    return 1;
}

std::vector<char> KafkaWriterStateSerializer::serialize(const KafkaWriterState& state) const {
    return {};
}

KafkaWriterState KafkaWriterStateSerializer::deserialize(int version, const std::vector<char>& serialized) const {
    return KafkaWriterState({});
}
