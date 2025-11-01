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
#ifndef FLINK_TNEL_FSCHECKPOINTSTREAMFACTORY_H
#define FLINK_TNEL_FSCHECKPOINTSTREAMFACTORY_H

#include "runtime/state/CheckpointStreamFactory.h"
#include "core/fs/Path.h"
#include "core/include/common.h"
#include "FsCheckpointStateOutputStream.h"

class FsCheckpointStreamFactory : public CheckpointStreamFactory {
public:
    FsCheckpointStreamFactory(int fileSystem, Path *checkpointDirectory, Path *sharedStateDirectory,
                              int fileStateSizeThreshold,
                              int writeBufferSize);

    ~FsCheckpointStreamFactory() = default;

    CheckpointStateOutputStream* createCheckpointStateOutputStream(CheckpointedStateScope scope) override;
    
    Path* getTargetPath(CheckpointedStateScope scope) const;

    bool canFastDuplicate(
        StreamStateHandle *stateHandle,
        CheckpointedStateScope scope) override
    {
        NOT_IMPL_EXCEPTION
    };

    std::vector<std::shared_ptr<StreamStateHandle>>
    duplicate(std::vector<StreamStateHandle *> &stateHandles,
              CheckpointedStateScope scope) override
    {
        NOT_IMPL_EXCEPTION
    };

private:
    int writeBufferSize;
    int fileStateThreshold;
    Path *checkpointDirectory;
    Path *sharedStateDirectory;
    int fileSystem; // NOT IMPLEMENTED
    static const int MAX_FILE_STATE_THRESHOLD = 1024 * 1024;
};

#endif // FLINK_TNEL_FSCHECKPOINTSTREAMFACTORY_H
