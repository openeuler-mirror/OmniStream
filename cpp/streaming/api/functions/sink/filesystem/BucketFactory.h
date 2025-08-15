/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BUCKET_FACTORY_H
#define OMNISTREAM_BUCKET_FACTORY_H

#include "Bucket.h"

template <typename IN, typename BucketID>
class BucketFactory {
public:
    Bucket<IN, BucketID> *getNewBucket(
        int subtaskIndex,
        const BucketID &bucketId,
        std::string bucketPath,
        long initialPartCounter,
        BucketWriter<IN, BucketID> *bucketWriter,
        RollingPolicy<IN, BucketID> *rollingPolicy,
        OutputFileConfig *outputFileConfig,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes)
    {
        return Bucket<IN, BucketID>::getNew(
            subtaskIndex,
            bucketId,
            bucketPath,
            initialPartCounter,
            bucketWriter,
            rollingPolicy,
            outputFileConfig,
            nonPartitionIndexes,
            inputTypes);
    }
};

#endif // OMNISTREAM_BUCKET_FACTORY_H