/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_KAFKASOURCEFETCHERMANAGER_H
#define FLINK_TNEL_KAFKASOURCEFETCHERMANAGER_H

#include <iostream>
#include <memory>
#include <map>
#include <functional>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "SingleThreadFetcherManager.h"


class KafkaSourceFetcherManager : public SingleThreadFetcherManager<RdKafka::Message, KafkaPartitionSplit> {
public:
    KafkaSourceFetcherManager(
            std::shared_ptr<FutureCompletingBlockingQueue<RdKafka::Message>>& elementsQueue,
            std::function<std::shared_ptr<SplitReader<RdKafka::Message, KafkaPartitionSplit>>()>& splitReaderSupplier);
};


#endif // FLINK_TNEL_KAFKASOURCEFETCHERMANAGER_H
