/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include <stdexcept>
#include <utility>
#include "TransactionalIdFactory.h"
#include "TransactionAborter.h"

TransactionAborter::TransactionAborter(std::function
    <std::shared_ptr<FlinkKafkaInternalProducer>(const std::string&)> producerFactory,
                                       std::function
    <void(std::shared_ptr<FlinkKafkaInternalProducer>)> closeAction)
    : producerFactory_(std::move(producerFactory)),
    closeAction_(std::move(closeAction)) {
}

void TransactionAborter::abortLingeringTransactions(const std::vector<std::string>& prefixesToAbort,
                                                    long startCheckpointId)
{
    for (const std::string& prefix : prefixesToAbort) {
        abortTransactionsWithPrefix(prefix, startCheckpointId);
    }
}

void TransactionAborter::abortTransactionsWithPrefix(const std::string& prefix, long startCheckpointId) {}

int TransactionAborter::abortTransactionOfSubtask(const std::string& prefix,
                                                  long startCheckpointId,
                                                  int subId)
{
    int numTransactionAborted = 0;
    for (long checkpointId = startCheckpointId; ; checkpointId++, numTransactionAborted++) {
        std::string transactionalId =
                TransactionalIdFactory::buildTransactionalId(prefix, subId, checkpointId);
        if (producer == nullptr) {
            producer = producerFactory_(transactionalId);
        } else {
            producer->initTransactionId(transactionalId);
        }
        producer->Flush();
        if (producer->getEpoch() == 0) {
            break;
        }
    }
    return numTransactionAborted;
}

void TransactionAborter::Close()
{
    if (producer != nullptr) {
        closeAction_(producer);
    }
}
