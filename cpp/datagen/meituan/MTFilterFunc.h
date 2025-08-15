/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_MTFILTERFUNC_H
#define OMNISTREAM_MTFILTERFUNC_H

#include <vector>
#include <functions/FilterFunction.h>
#include "OriginalRecord.h"
#include "vectorbatch/VectorBatch.h"

class MTFilterFunc : public BatchFilterFunction<void*> {
public:
    MTFilterFunc() : filterColIdx(1), filterResult(filterFlag)
    {
        filterFlag = !filterFlag;
    }

    std::vector<int> filterBatch(void* input) override
    {
        auto *batch = reinterpret_cast<omnistream::VectorBatch*>(input);
        std::vector<int> newBatch;
        auto rowCount = batch->GetRowCount();
        newBatch.reserve(rowCount);
        auto colVecs = batch->GetVectors();
        auto colVec = reinterpret_cast<omniruntime::vec::Vector<bool> *>(colVecs[filterColIdx]);
        for (int i = 0; i < rowCount; i++) {
            if (colVec->GetValue(i) == filterResult) {
                newBatch.push_back(i);
            }
        }
        return newBatch;
    }

private:
    static bool filterFlag;
    int filterColIdx;
    bool filterResult;
};


#endif // OMNISTREAM_MTFILTERFUNC_H
