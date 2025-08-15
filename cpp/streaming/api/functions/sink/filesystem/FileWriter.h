/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_IN_PROGRESS_FILE_WRITER_H
#define OMNISTREAM_IN_PROGRESS_FILE_WRITER_H

#include <iostream>
#include <fstream>
#include "table/data/util/VectorBatchUtil.h"

// This class combines InProgressFileWriter and PartFileInfo.
template <typename IN, typename BucketID>
class FileWriter {
public:
    FileWriter(const std::string &path,
        long createdTime,
        std::vector<int> nonPartitionIndexes,
        std::vector<std::string> inputTypes) : filePath(path),
                                               createdTime(createdTime),
                                               nonPartitionIndexes(nonPartitionIndexes),
                                               inputTypes(inputTypes)
    {
        outFile.open(filePath, std::ios::out | std::ios::app);
    }

    void write(IN batch, int rowId, long currentTime)
    {
        if (!outFile.is_open()) {
            std::cerr << "Error: Output file is not open!" << std::endl;
            return;
        }

        auto vb = reinterpret_cast<omnistream::VectorBatch *>(batch);
        for (size_t i = 0; i < nonPartitionIndexes.size(); ++i) {
            size_t colId = nonPartitionIndexes[i];
            auto val = VectorBatchUtil::getValueAtAsStr(vb, colId, rowId, inputTypes);
            outFile << val;

            if (colId != nonPartitionIndexes.size() - 1) {
                outFile << ",";
            }
        }
        outFile << "\n";
        lastUpdatedTime = currentTime;
    };

    void flush()
    {
        outFile.flush();
    }

    void close()
    {
        if (outFile.is_open()) {
            outFile.close();
        }
    }

    int64_t getSize()
    {
        if (!outFile.is_open()) {
            return 0;
        }
        auto pos = outFile.tellp();
        return pos == std::streampos(-1) ? 0 : static_cast<int64_t>(pos);
    }

    long getCreationTime()
    {
        return createdTime;
    }

    long getLastUpdateTime()
    {
        return lastUpdatedTime;
    }

    ~FileWriter()
    {
        LOG("FileWriter => Destructor")
        close();
    }

private:
    std::string filePath;
    std::ofstream outFile;
    long createdTime;
    long lastUpdatedTime;
    std::vector<int> nonPartitionIndexes;
    std::vector<std::string> inputTypes = {};
};

#endif // OMNISTREAM_IN_PROGRESS_FILE_WRITER_H