/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_STATE_H
#define FLINK_TNEL_STATE_H

#include "table/vectorbatch/VectorBatch.h"

class State {
public:
    State() : vectorBatches(){};
    virtual ~State()
    {
        for (auto batch : vectorBatches) {
            delete batch;
        }
    };
    virtual void clear() = 0;
    virtual void addVectorBatch(omnistream::VectorBatch *vectorBatch)
    {
        vectorBatches.push_back(vectorBatch);
    };
    const std::vector<omnistream::VectorBatch *> &getVectorBatches() const
    {
        return vectorBatches;
    };
    virtual long getVectorBatchesSize()
    {
        return vectorBatches.size();
    };
    virtual omnistream::VectorBatch *getVectorBatch(int batchId)
    {
        return vectorBatches[batchId];
    }
    void clearVectors(int64_t currentTimestamp)
    {
        for (size_t i = 0; i < vectorBatches.size(); ++i) {
            if (vectorBatches[i] && vectorBatches[i]->isEmpty(currentTimestamp)) {
                delete vectorBatches[i];
                vectorBatches[i] = nullptr;
            }
        }
    }

protected:
    std::vector<omnistream::VectorBatch *> vectorBatches;
};

#endif  // FLINK_TNEL_STATE_H
