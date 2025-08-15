/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "KafkaCommittableSerializer.h"

int KafkaCommittableSerializer::GetVersion()
{
    return 1;
}

std::vector<char> KafkaCommittableSerializer::serialize(KafkaCommittable* state) { return {}; }

KafkaCommittable* KafkaCommittableSerializer::deserialize(int version, const std::vector<char>& serialized) { return nullptr; }
