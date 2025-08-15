/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BUCKETS_H
#define OMNISTREAM_BUCKETS_H

#include <map>
#include <string>
#include "Bucket.h"
#include "BucketFactory.h"
#include "BucketAssigner.h"

class BucketerContext : public BucketAssignerContext {
    long elementTimestamp;
    long currentWatermark;
    long currentProcessingTime;
public:
    void update(long elementTimestamp, long watermark, long processingTime)
    {
        this->elementTimestamp = elementTimestamp;
        this->currentWatermark = watermark;
        this->currentProcessingTime = processingTime;
    }

    long getCurrentProcessingTime() const override
    {
        return currentProcessingTime;
    }

    long getCurrentWatermark() const override
    {
        return currentWatermark;
    }

    long getTimestamp() const override
    {
        return elementTimestamp;
    }
};

template <typename IN, typename BucketID>
class Buckets {
public:
    Buckets(std::string basePath,
            BucketAssigner<IN, BucketID> *bucketAssigner,
            BucketFactory<IN, BucketID> *bucketFactory,
            BucketWriter<IN, BucketID> *bucketWriter,
            RollingPolicy<IN, BucketID> *rollingPolicy,
            int subtaskIndex,
            OutputFileConfig *outputFileConfig,
            std::vector<int> nonPartitionIndexes,
            std::vector<std::string> inputTypes)
        : basePath(basePath),
          bucketFactory(bucketFactory),
          bucketAssigner(bucketAssigner),
          bucketWriter(bucketWriter),
          rollingPolicy(rollingPolicy),
          subtaskIndex(subtaskIndex),
          maxPartCounter(0),
          outputFileConfig(outputFileConfig),
          nonPartitionIndexes(nonPartitionIndexes),
          inputTypes(inputTypes) {}

    ~Buckets()
    {
        for (auto &entry : activeBuckets) {
            delete entry.second;
        }
    }

    Bucket<IN, BucketID> *onElement(IN batch, int rowId, long currentProcessingTime, long elementTimestamp, long currentWatermark) {
        bucketerContext.update(elementTimestamp, currentWatermark, currentProcessingTime);
        BucketID bucketId = bucketAssigner->getBucketId(batch, rowId, &bucketerContext);
        auto bucket = getOrCreateBucketForBucketId(bucketId);
        bucket->write(batch, rowId, currentProcessingTime);
        this->maxPartCounter = std::max(maxPartCounter, bucket->getPartCounter());
        return bucket;
    }

    void onProcessingTime(long timestamp)
    {
        for (auto &entry : activeBuckets) {
            entry.second->onProcessingTime(timestamp);
        }
    }

    void close()
    {
        for (auto &entry : activeBuckets) {
            delete entry.second;
        }
    }

private:
    std::string basePath;
    BucketFactory<IN, BucketID> *bucketFactory;
    BucketAssigner<IN, BucketID> *bucketAssigner;
    BucketWriter<IN, BucketID> *bucketWriter;
    RollingPolicy<IN, BucketID> *rollingPolicy;
    int subtaskIndex;
    long maxPartCounter;
    OutputFileConfig *outputFileConfig;
    std::vector<int> nonPartitionIndexes;
    std::vector<std::string> inputTypes;
    std::map<BucketID, Bucket<IN, BucketID> *> activeBuckets;
    BucketerContext bucketerContext;
    Bucket<IN, BucketID> *getOrCreateBucketForBucketId(BucketID bucketId)
    {
        auto it = activeBuckets.find(bucketId);
        if (it != activeBuckets.end())
        {
            return it->second;
        }
        std::string bucketPath = assembleBucketPath(bucketId);
        Bucket<IN, BucketID> *bucket = bucketFactory->getNewBucket(
            subtaskIndex, bucketId, bucketPath, maxPartCounter,
            bucketWriter, rollingPolicy, outputFileConfig, nonPartitionIndexes, inputTypes);

        activeBuckets[bucketId] = bucket;
        return bucket;
    }

    std::string assembleBucketPath(BucketID bucketId)
    {
        return bucketId.empty() ? basePath : basePath + "/" + bucketId;
    }
};

#endif // OMNISTREAM_BUCKETS_H