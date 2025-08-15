/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "KafkaSourceFetcherManager.h"

KafkaSourceFetcherManager::KafkaSourceFetcherManager(
    std::shared_ptr<FutureCompletingBlockingQueue<RdKafka::Message>>& elementsQueue,
    std::function<std::shared_ptr<SplitReader<RdKafka::Message, KafkaPartitionSplit>>()>& splitReaderSupplier)
    : SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit>(elementsQueue, splitReaderSupplier) {}