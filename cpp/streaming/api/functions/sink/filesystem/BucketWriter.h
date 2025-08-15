/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_BUCKET_WRITER_H
#define OMNISTREAM_BUCKET_WRITER_H

#include <string>
#include <filesystem>
#include "FileWriter.h"

namespace fs = std::filesystem;

template <typename IN, typename BucketID>
class BucketWriter {
public:
    FileWriter<IN, BucketID> *openNewInProgressFile(BucketID bucketId, const std::string &path, long currentTime, std::vector<int> nonPartitionIndexes, std::vector<std::string> inputTypes)
    {
        fs::path targetPath(path);
        fs::path parentDir = targetPath.parent_path();

        if (!fs::exists(parentDir))
        {
            std::error_code ec;
            if (!fs::create_directories(parentDir, ec))
            {
                throw std::ios_base::failure("Failed to create directory: " + parentDir.string() + ", error: " + ec.message());
            }
        }

        return new FileWriter<IN, BucketID>(path, currentTime, nonPartitionIndexes, inputTypes);
    }
};

#endif // OMNISTREAM_BUCKET_WRITER_H