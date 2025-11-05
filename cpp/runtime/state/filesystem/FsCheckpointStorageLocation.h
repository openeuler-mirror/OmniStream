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
#ifndef FLINK_TNEL_FSCHECKPOINTSTORAGELOCATION_H
#define FLINK_TNEL_FSCHECKPOINTSTORAGELOCATION_H
#include "core/fs/Path.h"
#include "runtime/state/CheckpointStorageLocationReference.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "FsCheckpointStreamFactory.h"
#include "runtime/state/filesystem/AbstractFsCheckpointStorageAccess.h"

namespace omnistream {

class FsCheckpointStorageLocation : public FsCheckpointStreamFactory {
public:
    FsCheckpointStorageLocation(
        int fileSystem,
        Path *checkpointDir,
        Path *sharedStateDir,
        Path *taskOwnedStateDir,
        const CheckpointStorageLocationReference reference,
        int fileStateSizeThreshold,
        int writeBufferSize)
        : FsCheckpointStreamFactory(fileSystem, checkpointDir, sharedStateDir, fileStateSizeThreshold, writeBufferSize),
          fileSystem(fileSystem),
          checkpointDirectory(checkpointDir),
          sharedStateDirectory(sharedStateDir),
          taskOwnedStateDirectory(taskOwnedStateDir),
          reference(reference),
          fileStateSizeThreshold(fileStateSizeThreshold),
          writeBufferSize(writeBufferSize)
    {
        if (fileStateSizeThreshold < 0) {
            THROW_LOGIC_EXCEPTION(
                "The threshold for file state size must be zero or larger");
        }

        if (writeBufferSize < 0) {
            THROW_LOGIC_EXCEPTION(
                "The write buffer size must be zero or larger");
        }

        Path metadataDir = Path(*checkpointDir); // TTODO EntropyInjector.removeEntropyMarkerIfPresent
        metadataFilePath = new Path(metadataDir, AbstractFsCheckpointStorageAccess::METADATA_FILE_NAME);
    }

    Path *getCheckpointDirectory()
    {
        return checkpointDirectory;
    }

    Path *getSharedStateDirectory()
    {
        return sharedStateDirectory;
    }

    Path *getTaskOwnedStateDirectory()
    {
        return taskOwnedStateDirectory;
    }

    Path *getMetadataFilePath()
    {
        return metadataFilePath;
    }

private:
    int fileSystem;
    Path *checkpointDirectory;
    Path *sharedStateDirectory;
    Path *taskOwnedStateDirectory;
    Path *metadataFilePath;
    const CheckpointStorageLocationReference &reference;
    int fileStateSizeThreshold;
    int writeBufferSize;
};

}
#endif // FLINK_TNEL_FSCHECKPOINTSTORAGELOCATION_H
