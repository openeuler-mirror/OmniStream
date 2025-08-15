/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_STREAMING_FILE_SINK_H
#define OMNISTREAM_STREAMING_FILE_SINK_H

#include "./Buckets.h"
#include "./BucketWriter.h"
#include "./BulkWriter.h"
#include "./Path.h"
#include "./OutputFileConfig.h"

template <typename IN, typename BucketID>
class BucketsBuilder {
public:
    virtual ~BucketsBuilder() = default;
    virtual Buckets<IN, BucketID> *createBuckets(int subtaskIndex) = 0;
    virtual BucketWriter<IN, BucketID> *createBucketWriter() = 0;
};

template <typename IN, typename BucketID>
class BulkFormatBuilder : public BucketsBuilder<IN, BucketID>
{
private:
    Path basePath;
    BucketAssigner<IN, BucketID> *bucketAssigner;
    RollingPolicy<IN, BucketID> *rollingPolicy;
    BucketFactory<IN, BucketID> *bucketFactory;
    OutputFileConfig outputFileConfig;
    std::vector<int> nonPartitionIndexes;
    std::vector<std::string> inputTypes;

    int64_t parseDuration(const std::string &durationStr)
    {
        if (durationStr.empty())
            return 0;
        size_t pos = 0;
        int value = std::stoi(durationStr, &pos);
        std::string unit;
        while (pos < durationStr.size() && std::isspace(durationStr[pos])) {
            pos++;
        }
        for (; pos < durationStr.size(); pos++) {
            unit += std::tolower(durationStr[pos]);
        }

        if (unit.empty() || unit == "ms") {
            return value;
        } else if (unit == "s" || unit == "sec" || unit == "secs") {
            return value * 1000;
        } else if (unit == "m" || unit == "min" || unit == "mins") {
            return value * 60 * 1000;
        } else if (unit == "h" || unit == "hr" || unit == "hrs") {
            return value * 60 * 60 * 1000;
        } else {
            return value;
        }
    }

    int64_t parseFileSize(const std::string &fileSizeStr)
    {
        const int64_t DEFAULT_FILE_SIZE = 128LL * 1024LL * 1024LL; // 128MB
        if (fileSizeStr.empty())
            return DEFAULT_FILE_SIZE;
        size_t pos = 0;
        int value = std::stoi(fileSizeStr, &pos);
        std::string unit;
        while (pos < fileSizeStr.size() && std::isspace(fileSizeStr[pos])) {
            pos++;
        }
        for (; pos < fileSizeStr.size(); pos++) {
            unit += std::tolower(fileSizeStr[pos]);
        }

        if (unit == "kb" || unit == "k") {
            return value * 1024LL;
        } else if (unit == "mb" || unit == "m") {
            return value * (1024LL * 1024LL);
        } else if (unit == "gb" || unit == "g") {
            return value * (1024LL * 1024LL * 1024LL);
        } else if (unit == "tb" || unit == "t") {
            return value * (1024LL * 1024LL * 1024LL * 1024LL);
        } else {
            return value;
        }
    }

public:
    BulkFormatBuilder(
        const Path &basePath,
        BucketAssigner<IN, BucketID> *assigner,
        RollingPolicy<IN, BucketID> *policy,
        BucketFactory<IN, BucketID> *factory,
        const OutputFileConfig &outputFile,
        std::vector<int> nonPartitionIndexes)
        : basePath(basePath),
          bucketAssigner(assigner),
          rollingPolicy(policy),
          bucketFactory(factory),
          outputFileConfig(outputFile),
          nonPartitionIndexes(nonPartitionIndexes) {}

    BulkFormatBuilder(const nlohmann::json &config)
        : basePath(config["path"].get<std::string>()),
          outputFileConfig("output", ".txt")
    {
        rollingPolicy = new RollingPolicy<IN, BucketID>(
            parseFileSize(config["rolling-policy"]["file-size"].get<std::string>()),
            parseDuration(config["rolling-policy"]["rollover-interval"].get<std::string>()),
            parseDuration(config["rolling-policy"]["check-interval"].get<std::string>()));

        auto partitionKeys = config["partitionKeys"].get<std::vector<std::string>>();
        auto partitionIndexes = config["partitionIndexes"].get<std::vector<int>>();

        nonPartitionIndexes = config["nonPartitionIndexes"].get<std::vector<int>>();
        inputTypes = config["inputTypes"].get<std::vector<std::string>>();
        bucketAssigner = new BucketAssigner<IN, BucketID>(partitionKeys, partitionIndexes);
        bucketFactory = new BucketFactory<IN, BucketID>();
    }

    BucketWriter<IN, BucketID> *createBucketWriter()
    {
        return new BucketWriter<IN, BucketID>();
    }

    Buckets<IN, BucketID> *createBuckets(int subtaskIndex) override
    {
        return new Buckets<IN, BucketID>(
            basePath.toString(),
            bucketAssigner,
            bucketFactory,
            createBucketWriter(),
            rollingPolicy,
            subtaskIndex,
            &outputFileConfig,
            nonPartitionIndexes,
            inputTypes);
    }

    ~BulkFormatBuilder()
    {
        delete bucketAssigner;
        delete rollingPolicy;
        delete bucketFactory;
    }
};

#endif // OMNISTREAM_STREAMING_FILE_SINK_H