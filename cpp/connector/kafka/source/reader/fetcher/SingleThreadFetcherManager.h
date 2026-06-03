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

#ifndef FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H
#define FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H

#include <vector>
#include <memory>
#include <functional>
#include <queue>
#include <map>
#include "SplitFetcherManager.h"
#include "core/include/common.h"

template <typename E, typename SplitT>
class SingleThreadFetcherManager : public SplitFetcherManager<E, SplitT> {
public:
    SingleThreadFetcherManager(
        FutureCompletingBlockingQueue<E>* elementsQueue,
        std::function<SplitReader<E, SplitT>*()>& splitReaderSupplier)
        : SplitFetcherManager<E, SplitT>(elementsQueue, splitReaderSupplier) {}

    void addSplits(std::vector<SplitT*>& splitsToAdd) override
    {
        INFO_RELEASE("[OS-source-fetcher] manager addSplits begin, manager="
            << reinterpret_cast<uintptr_t>(this)
            << ", splits=" << splitsToAdd.size()
            << ", aliveFetchers=" << this->getNumAliveFetchers());
        auto fetcher = getRunningFetcher();
        if (fetcher == nullptr) {
            fetcher = this->createSplitFetcher();
            INFO_RELEASE("[OS-source-fetcher] created fetcher, manager="
                << reinterpret_cast<uintptr_t>(this)
                << ", fetcher=" << reinterpret_cast<uintptr_t>(fetcher)
                << ", splits=" << splitsToAdd.size());
            // Add the splits to the fetchers.
            fetcher->addSplits(splitsToAdd);
            this->startFetcher(fetcher);
        } else {
            INFO_RELEASE("[OS-source-fetcher] reuse fetcher, manager="
                << reinterpret_cast<uintptr_t>(this)
                << ", fetcher=" << reinterpret_cast<uintptr_t>(fetcher)
                << ", splits=" << splitsToAdd.size());
            fetcher->addSplits(splitsToAdd);
        }
        INFO_RELEASE("[OS-source-fetcher] manager addSplits end, manager="
            << reinterpret_cast<uintptr_t>(this)
            << ", splits=" << splitsToAdd.size());
    }
protected:
    SplitFetcher<E, SplitT>* getRunningFetcher()
    {
        return this->getNumAliveFetchers() == 0 ? nullptr : this->fetchers.begin()->second;
    }
};

#endif // FLINK_TNEL_SINGLETHREADFETCHERMANAGER_H
