/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_KAFKAWRITERSTATESERIALIZER_H
#define FLINK_BENCHMARK_KAFKAWRITERSTATESERIALIZER_H

#include <string>
#include <vector>
#include <stdexcept>

#include "KafkaWriterState.h"

class KafkaWriterStateSerializer {
public:
    int GetVersion() const;
    std::vector<char> serialize(const KafkaWriterState& state) const;
    KafkaWriterState deserialize(int version, const std::vector<char>& serialized) const;
};

#endif // FLINK_BENCHMARK_KAFKAWRITERSTATESERIALIZER_H
