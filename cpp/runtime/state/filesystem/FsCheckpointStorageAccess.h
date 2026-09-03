/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * We modify this part of the code based on Apache Flink to implement native execution of Flink operators.
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#ifndef FLINK_TNEL_FSCHECKPOINTSTORAGEACCESS_H
#define FLINK_TNEL_FSCHECKPOINTSTORAGEACCESS_H
#include "AbstractFsCheckpointStorageAccess.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "FsCheckpointStorageLocation.h"

namespace omnistream {

class FsCheckpointStorageAccess : public AbstractFsCheckpointStorageAccess {
public:
    FsCheckpointStorageAccess(
        std::shared_ptr<Path> checkpointBaseDirectory,
        std::shared_ptr<Path> defaultSavepointDirectory,
        JobIDPOD jobId,
        int fileSizeThreshold,
        int writeBufferSize)
        : FsCheckpointStorageAccess(
              checkpointBaseDirectory->getFileSystem(),
              checkpointBaseDirectory,
              defaultSavepointDirectory,
              jobId,
              fileSizeThreshold,
              writeBufferSize)
    {
    }

    FsCheckpointStorageAccess(
        int fs,
        std::shared_ptr<Path> checkpointBaseDirectory,
        std::shared_ptr<Path> defaultSavepointDirectory,
        JobIDPOD jobId,
        int fileSizeThreshold,
        int writeBufferSize)
    {
        if (fileSizeThreshold < 0) {
            THROW_LOGIC_EXCEPTION("fileSizeThreshold cannot be less than zero");
        }
        if (writeBufferSize < 0) {
            THROW_LOGIC_EXCEPTION("writeBufferSize cannot be less than zero");
        }

        fileSystem = fs;
        checkpointsDirectory = getCheckpointDirectoryForJob(*checkpointBaseDirectory, jobId);
        sharedStateDirectory = std::make_shared<Path>(*checkpointsDirectory, CHECKPOINT_SHARED_STATE_DIR);
        taskOwnedStateDirectory = std::make_shared<Path>(*checkpointsDirectory, CHECKPOINT_TASK_OWNED_STATE_DIR);
        this->fileSizeThreshold = fileSizeThreshold;
        this->writeBufferSize = writeBufferSize;
    }

    CheckpointStateOutputStream* createTaskOwnedStateStream() override
    {
        return nullptr;
    };

    CheckpointStateToolset* createTaskOwnedCheckpointStateToolset() override
    {
        return nullptr;
    };

    std::shared_ptr<CheckpointStreamFactory> resolveCheckpointStorageLocation(
        int64_t checkpointId, std::shared_ptr<CheckpointStorageLocationReference> reference) override
    {
        if (reference->IsDefaultReference()) {
            std::shared_ptr<Path> checkpointDir = createCheckpointDirectory(*checkpointsDirectory, checkpointId);

            return std::make_shared<FsCheckpointStorageLocation>(
                fileSystem,
                checkpointDir,
                sharedStateDirectory,
                taskOwnedStateDirectory,
                reference,
                fileSizeThreshold,
                writeBufferSize);
        } else {
            std::shared_ptr<Path> path = decodePathFromReference(reference);

            return std::make_shared<FsCheckpointStorageLocation>(
                path->getFileSystem(), path, path, path, reference, fileSizeThreshold, writeBufferSize);
        }
    };

    CheckpointStorageLocation* initializeLocationForCheckpoint(long checkpointId) override
    {
        return nullptr; // NOT_IMPLEMENTED
    };

    CheckpointStorageLocation* initializeLocationForSavepoint(
        long checkpointId, const std::string& externalPath) override
    {
        return nullptr; // NOT_IMPLEMENTED
    }

    bool hasDefaultSavepointLocation() override
    {
        return true; // NOT_IMPLEMENTED
    }

    std::shared_ptr<CheckpointStreamFactory> resolveCheckpointStorageLocation(long checkpointId) override
    {
        return nullptr; // NOT_IMPLEMENTED
    }

    void initializeBaseLocations() override
    {
    }

private:
    int fileSystem;
    std::shared_ptr<Path> checkpointsDirectory;
    std::shared_ptr<Path> sharedStateDirectory;
    std::shared_ptr<Path> taskOwnedStateDirectory;
    int fileSizeThreshold;
    int writeBufferSize;
};

} // namespace omnistream

#endif // FLINK_TNEL_FSCHECKPOINTSTORAGEACCESS_H
