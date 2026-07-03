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
    virtual void addVectorBatch(omnistream::VectorBatch* vectorBatch)
    {
        vectorBatches.push_back(vectorBatch);
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

protected:
    std::vector<omnistream::VectorBatch*> vectorBatches;
};
