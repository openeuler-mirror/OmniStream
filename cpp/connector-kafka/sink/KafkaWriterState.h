/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_KAFKAWRITERSTATE_H
#define FLINK_BENCHMARK_KAFKAWRITERSTATE_H

#include <string>
#include <memory>

class KafkaWriterState {
public:
    explicit KafkaWriterState(const std::string& transactionalIdPrefix);

    std::string getTransactionalIdPrefix() const;

    bool operator==(const KafkaWriterState& that) const;

    bool operator!=(const KafkaWriterState& that) const;

    size_t hashCode() const;

    std::string toString() const;

private:
    std::string transactionalIdPrefix;
};
#endif // FLINK_BENCHMARK_KAFKAWRITERSTATE_H
