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
