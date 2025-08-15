/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H
#define FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H

#include <vector>
#include <memory>
#include <functional>
#include <queue>
#include <map>
#include "SplitFetcherManager.h"

template <typename E, typename SplitT>
class SingleThreadFetcherManager : public SplitFetcherManager<E, SplitT> {
public:
    SingleThreadFetcherManager(
        std::shared_ptr<FutureCompletingBlockingQueue<E>>& elementsQueue,
        std::function<std::shared_ptr<SplitReader<E, SplitT>>()>& splitReaderSupplier)
        : SplitFetcherManager<E, SplitT>(elementsQueue, splitReaderSupplier) {}

    void addSplits(std::vector<SplitT*>& splitsToAdd) override
    {
        auto fetcher = getRunningFetcher();
        if (fetcher == nullptr) {
            fetcher = this->createSplitFetcher();
            // Add the splits to the fetchers.
            fetcher->addSplits(splitsToAdd);
            this->startFetcher(fetcher);
        } else {
            fetcher->addSplits(splitsToAdd);
        }
    }
protected:
    std::shared_ptr<SplitFetcher<E, SplitT>> getRunningFetcher()
    {
        return this->getNumAliveFetchers() == 0 ? nullptr : this->fetchers.begin()->second;
    }
};

#endif // FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H
