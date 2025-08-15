/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_FLINKKAFKAINTERNALPRODUCER_H
#define FLINK_BENCHMARK_FLINKKAFKAINTERNALPRODUCER_H

#include <chrono>
#include <string>
#include <atomic>
#include <memory>
#include <librdkafka/rdkafkacpp.h>
#include "core/include/common.h"

class FlinkKafkaInternalProducer {
public:
    FlinkKafkaInternalProducer(RdKafka::Conf* properties, const std::string& transactionalId);
    ~FlinkKafkaInternalProducer();
    void Flush();
    void BeginTransaction();
    void AbortTransaction();
    void CommitTransaction();
    bool IsInTransaction() const;
    void Close();
    void Close(int timeout);
    bool IsClosed() const;
    const std::string& getTransactionalId() const;
    int32_t getEpoch() const;
    int64_t getProducerId() const;
    void initTransactionId(const std::string& transactionalId);
    void setTransactionId(const std::string& transactionalId);
    void resumeTransaction(int64_t producerId, int32_t epoch);
    RdKafka::Producer* getKafkaProducer();

private:
    static RdKafka::Conf* withTransactionalId(RdKafka::Conf* properties, const std::string& transactionalId);
    void FlushNewPartitions();
    void* GetTransactionManager() const;
    void transitionTransactionManagerStateTo(const std::string& state);
    void* createProducerIdAndEpoch(int64_t producerId, int32_t epoch);

    RdKafka::Producer *producer_;
    std::string transactionalId_;
    std::atomic<bool> inTransaction_;
    std::atomic<bool> closed_;
    std::string errstr;
    int timeout_ = 1000;
    int pollTimeout_ = 60 * 60 * 1000;
};

#endif // FLINK_BENCHMARK_FLINKKAFKAINTERNALPRODUCER_H
