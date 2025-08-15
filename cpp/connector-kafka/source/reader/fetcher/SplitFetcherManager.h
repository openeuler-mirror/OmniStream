/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SPLITFETCHERMANAGER_H
#define FLINK_TNEL_SPLITFETCHERMANAGER_H

#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <stdexcept>
#include <chrono>
#include "connector-kafka/source/reader/RecordsWithSplitIds.h"
#include "connector-kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceSplit.h"
#include "SplitFetcher.h"

// todo concurrentHashmap 用map和锁来替换

template <typename E, typename SplitT>
class SplitFetcherManager {
public:
    SplitFetcherManager(std::shared_ptr<FutureCompletingBlockingQueue<E>>& elementsQueue,
        std::function<std::shared_ptr<SplitReader<E, SplitT>>()>& splitReaderFactory)
        : elementsQueue(elementsQueue), splitReaderFactory(splitReaderFactory)
    {
        errorHandler = [this, elementsQueue](const std::exception_ptr& e) {
            try {
                if (e) {
                    std::rethrow_exception(e);
                }
            } catch (const std::exception& ex) {
                std::cerr << ": Received uncaught exception: " << ex.what() << std::endl;
            }
            // 异常信息保存
            std::lock_guard<std::mutex> lock(exceptionMutex);
            if (!uncaughtFetcherException) {
                uncaughtFetcherException = e;
            } else {
                // 这里模拟 Java 的 addSuppressed 比较复杂，暂不详细实现
            }
            elementsQueue->notifyAvailable();
        };
    }

    // 抽象方法，派生类需要实现
    virtual void addSplits(std::vector<SplitT*>& splitsToAdd) = 0;

    // 启动一个 fetcher
    void startFetcher(const std::shared_ptr<SplitFetcher<E, SplitT>>& fetcher)
    {
        executorThreads.emplace_back([fetcher]() {
            fetcher->run();
        });
    }

    // 创建一个新的 split fetcher
    std::shared_ptr<SplitFetcher<E, SplitT>> createSplitFetcher()
    {
        std::lock_guard<std::mutex> lock(fetchersMutex);
        if (closed) {
            throw std::runtime_error("The split fetcher manager has closed.");
        }
        std::shared_ptr<SplitReader<E, SplitT>> splitReader = splitReaderFactory();
        int fetcherId = fetcherIdGenerator.fetch_add(1);
        auto fetcher = std::make_shared<SplitFetcher<E, SplitT>>(
            fetcherId, elementsQueue, splitReader, errorHandler,
            [this, fetcherId]() {
                std::lock_guard<std::mutex> lock(fetchersMutex);
                fetchers.erase(fetcherId);
                elementsQueue->notifyAvailable();
            });
        fetchers[fetcherId] = fetcher;
        return fetcher;
    }

    // 检查并关闭已经完成工作的 fetchers
    bool maybeShutdownFinishedFetchers()
    {
        std::lock_guard<std::mutex> lock(fetchersMutex);
        auto it = fetchers.begin();
        while (it != fetchers.end()) {
            auto& fetcher = it->second;
            if (fetcher->isIdle()) {
                LOG(": Closing splitFetcher " + std::to_string(it->first) + " because it is idle.");
                fetcher->shutdown();
                it = fetchers.erase(it);
            } else {
                ++it;
            }
        }
        return fetchers.empty();
    }

    // 关闭 split fetcher manager
    void close(long timeoutMs)
    {
            std::lock_guard<std::mutex> lock(fetchersMutex);
            closed = true;
            for (const auto& fetcher : fetchers) {
                fetcher.second->shutdown();
            }
    }

    void checkErrors()
    {
        if (!uncaughtFetcherException) {
            return;
        }
        try {
            std::rethrow_exception(uncaughtFetcherException);
        } catch (const std::exception& ex) {
            throw std::runtime_error("One or more fetchers have encountered exception: " + std::string(ex.what()));
        }
    }

    // 测试用，获取存活的 fetcher 数量
    int getNumAliveFetchers()
    {
        std::lock_guard<std::mutex> lock(fetchersMutex);
        return static_cast<int>(fetchers.size());
    }
protected:
    std::map<int, std::shared_ptr<SplitFetcher<E, SplitT>>> fetchers;
private:
    std::function<void(const std::exception_ptr&)> errorHandler;
    std::atomic<int> fetcherIdGenerator{0};
    std::shared_ptr<FutureCompletingBlockingQueue<E>> elementsQueue;
    std::function<std::shared_ptr<SplitReader<E, SplitT>>()> splitReaderFactory;
    std::exception_ptr uncaughtFetcherException;
    std::mutex exceptionMutex;
    std::vector<std::thread> executorThreads;
    std::atomic<bool> closed{false};
    std::mutex fetchersMutex;
};

#endif // FLINK_TNEL_SPLITFETCHERMANAGER_H
