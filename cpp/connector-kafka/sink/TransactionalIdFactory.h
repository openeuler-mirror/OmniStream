/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_TRANSACTIONALIDFACTORY_H
#define FLINK_BENCHMARK_TRANSACTIONALIDFACTORY_H

#include <string>

class TransactionalIdFactory {
public:
    static std::string buildTransactionalId(
            const std::string& transactionalIdPrefix, int subtaskId, long checkpointOffset);
private:
    static const std::string TRANSACTIONAL_ID_DELIMITER;
    static const int randomStrLen = 6;
};

#endif // FLINK_BENCHMARK_TRANSACTIONALIDFACTORY_H
