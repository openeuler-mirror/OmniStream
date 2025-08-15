/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BUCKET_ASSIGNER_H
#define OMNISTREAM_BUCKET_ASSIGNER_H

#include <nlohmann/json.hpp>
#include "vectorbatch/VectorBatch.h"
#include "table/utils/PartitionPathUtils.h"

class BucketAssignerContext {
public:
    virtual ~BucketAssignerContext() = default;
    virtual long getCurrentProcessingTime() const = 0;
    virtual long getCurrentWatermark() const = 0;
    virtual long getTimestamp() const = 0;
};

template <typename IN, typename BucketID>
class BucketAssigner {
public:
    BucketAssigner(std::vector<std::string> keys, std::vector<int> indexes) : partitionKeys(std::move(keys)),
                                                                              partitionIndexes(std::move(indexes))
    {
        if (partitionKeys.size() != partitionIndexes.size()) {
            throw std::runtime_error("mismatched keys/indexes size");
        }
    }

    BucketID getBucketId(IN batch, int rowId, BucketAssignerContext *context)
    {
        std::vector<std::pair<std::string, std::string>> partitionSpec;
        auto vb = reinterpret_cast<omnistream::VectorBatch *>(batch);

        for (size_t i = 0; i < partitionKeys.size(); ++i) {
            auto val = VectorBatchUtil::getValueAtAsStr(vb, partitionIndexes[i], rowId);
            partitionSpec.emplace_back(partitionKeys[i], std::string(val));
        }

        auto partitionPath = PartitionPathUtils::generatePartitionPath(partitionSpec);
        return partitionPath;
    };

private:
    std::vector<std::string> partitionKeys;
    std::vector<int> partitionIndexes;
};

#endif // OMNISTREAM_BUCKET_ASSIGNER_H