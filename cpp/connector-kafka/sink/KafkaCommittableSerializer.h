/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H
#define OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H

#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include "KafkaCommittable.h"

class KafkaCommittableSerializer {
public:
    int GetVersion();
    std::vector<char> serialize(KafkaCommittable* state);
    KafkaCommittable* deserialize(int version, const std::vector<char>& serialized);
};

#endif // OMNIFLINK_KAFKACOMMITTABLESERIALIZER_H
