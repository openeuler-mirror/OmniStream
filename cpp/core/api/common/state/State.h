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

#pragma once

#include <vector>
#include <atomic>
#include <cstdint>
#include <unordered_map>
#include "table/data/vectorbatch/VectorBatch.h"
namespace omnistream {
enum class StateType {
    HEAP = 0,
    ROCKSDB = 1,
    BSS = 2
};
} // namespace omnistream
class State {
public:
    State() : vectorBatches() {};
    virtual ~State()
    {
        for (auto batch : vectorBatches) {
            delete batch;
        }
    };
    virtual void clear() = 0;
    virtual void addVectorBatch(omnistream::VectorBatch *vectorBatch)
    {
        // vectorBatches.push_back(vectorBatch);
        // maintain running totals for the per-operator VectorBatch metrics. Written
        // only on the task thread (here / clearVectors); read via atomic loads on the metric-reporter
        // thread, so the reporter never iterates the live vectorBatches vector (avoids racing a
        // push_back realloc). vbCount_ counts only live (non-freed) batches.
        // vbDataSize_.fetch_add(vectorBatch ? vectorBatch->getSizeInBytes() : 0, std::memory_order_relaxed);
        // vbCount_.fetch_add(1, std::memory_order_relaxed);
    };

    const std::vector<omnistream::VectorBatch*>& getVectorBatches() const
    {
        return vectorBatches;
    };
    virtual long getVectorBatchesSize()
    {
        return vectorBatches.size();
    };
    virtual omnistream::VectorBatch* getVectorBatch(int batchId)
    {
        if (batchId < 0 || static_cast<size_t>(batchId) >= vectorBatches.size()) {
            THROW_LOGIC_EXCEPTION(
                "batchId out of bounds: batchId = " << batchId << ", vectorBatches.size() = " << vectorBatches.size());
        }
        return vectorBatches[batchId];
    }
    virtual void clearVectors(int64_t currentTimestamp)
    {
        for (size_t i = 0; i < vectorBatches.size(); ++i) {
            if (vectorBatches[i] && vectorBatches[i]->isEmpty(currentTimestamp)) {
                // subtract the freed batch from the running totals before delete.
                vbDataSize_.fetch_sub(vectorBatches[i]->getSizeInBytes(), std::memory_order_relaxed);
                vbCount_.fetch_sub(1, std::memory_order_relaxed);
                delete vectorBatches[i];
                vectorBatches[i] = nullptr;
            }
        }
    }

    virtual void clearVectors(std::vector<size_t>& indicesToDelete)
    {
        for (size_t index : indicesToDelete) {
            if (index < vectorBatches.size() && vectorBatches[index]) {
                delete vectorBatches[index];
                vectorBatches[index] = nullptr;
            }
        }
    }

    // reporter-thread-safe reads of the running VectorBatch totals (atomic loads, no
    // access to the live vectorBatches vector).
    int64_t getVbDataSize() const
    {
        return vbDataSize_.load(std::memory_order_relaxed);
    }
    int64_t getVbCount() const
    {
        return vbCount_.load(std::memory_order_relaxed);
    }

    void recordVbStatistic(omnistream::VectorBatch *vectorBatch)
    {
        vbDataSize_.fetch_add(vectorBatch ? vectorBatch->getSizeInBytes() : 0, std::memory_order_relaxed);
        vbCount_.fetch_add(1, std::memory_order_relaxed);
    }

protected:
    std::vector<omnistream::VectorBatch *> vectorBatches;
    // running totals of the live held VectorBatches (bytes and count). Task thread is
    // the only writer; the metric-reporter thread only loads them.
    std::atomic<int64_t> vbDataSize_{0};
    std::atomic<int64_t> vbCount_{0};
};