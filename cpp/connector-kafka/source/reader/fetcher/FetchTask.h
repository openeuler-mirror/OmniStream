/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_FETCHTASK_H
#define FLINK_TNEL_FETCHTASK_H

#include <iostream>
#include <memory>
#include <functional>
#include <vector>
#include <atomic>
#include <stdexcept>
#include <thread>
#include <map>
#include "connector-kafka/source/reader/RecordsWithSplitIds.h"
#include "connector-kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceSplit.h"
#include "connector-kafka/source/reader/KafkaPartitionSplitReader.h"
#include "SplitFetcherTask.h"
#include "connector-kafka/bind_core_manager.h"


template <typename E, typename SplitT>
class FetchTask : public SplitFetcherTask {
public:
    FetchTask(
            const std::shared_ptr<SplitReader<E, SplitT>>& splitReader,
            const std::shared_ptr<FutureCompletingBlockingQueue<E>>& elementsQueue,
            std::function<void(const std::set<std::string>&)> splitFinishedCallback,
            int fetcherIndex): splitReader(splitReader),
                               elementsQueue(elementsQueue),
                               splitFinishedCallback(splitFinishedCallback),
                               fetcherIndex(fetcherIndex),
                               lastRecords(nullptr) {}

    bool Run() override
    {
        if (unlikely(not binded)) {
            coreId = elementsQueue->getCoreId();
            if (coreId >= 0) {
                omnistream::BindCoreManager::GetInstance()->BindDirectCore(coreId);
            }
            binded = true;
        }
        try {
            fetch();
            // 异常类全局重写
        } catch (const std::exception& e) {
            throw std::runtime_error("Source fetch execution was interrupted: " + std::string(e.what()));
        }
        // finally 是否需要关注
        if (IsWakenUp()) {
            wakeup.store(false);
        }
        return true;
    }

    void fetch()
    {
        if (!IsWakenUp() && !lastRecords) {
            lastRecords = splitReader->fetch();
        }
        if (!IsWakenUp()) {
            if (elementsQueue->put(fetcherIndex, lastRecords)) {
                if (!lastRecords->finishedSplits().empty()) {
                    splitFinishedCallback(lastRecords->finishedSplits());
                }
                lastRecords = nullptr;
            }
        }
    }

    void WakeUp() override
    {
        wakeup.store(true);
        if (!lastRecords) {
            splitReader->wakeUp();
        } else {
            elementsQueue->wakeUpPuttingThread(fetcherIndex);
        }
    }

    bool IsWakenUp() const
    {
        return wakeup.load();
    }

    std::string ToString() override
    {
        return "FetchTask";
    }
private:
    const std::shared_ptr<SplitReader<E, SplitT>> splitReader;
    const std::shared_ptr<FutureCompletingBlockingQueue<E>> elementsQueue;
    const std::function<void(const std::set<std::string>&)> splitFinishedCallback;
    const int fetcherIndex;
    RecordsWithSplitIds<E>* lastRecords;
    std::atomic<bool> wakeup{false};
    int32_t coreId = -1;
    bool binded = false;
};

#endif // FLINK_TNEL_FETCHTASK_H
