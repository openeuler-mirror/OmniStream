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
#ifndef OMNISTREAM_FS_CHECKPOINT_STATE_OUTPUT_STREAM_H
#define OMNISTREAM_FS_CHECKPOINT_STATE_OUTPUT_STREAM_H

#include <ostream>
#include <string>
#include "core/utils/lang/AutoCloseable.h"
#include "core/fs/Path.h"
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/CheckpointStreamFactory.h"

class FsCheckpointStateOutputStream : public CheckpointStateOutputStream {
public:
    FsCheckpointStateOutputStream(Path basePath,
                                    int fs,
                                    int bufferSize,
                                    int localStateThreshold,
                                    bool allowRelativePaths = false);

    ~FsCheckpointStateOutputStream() = default;

    void Write(const void* data, size_t length);

    void Flush();

    void Close();

    StreamStateHandle* CloseAndGetHandle();

    bool IsClosed();

    long GetPos();

    void Sync();

private:
    Path basePath_;
    int fs_;
    int bufferSize_;
    int localStateThreshold_;
    bool allowRelativePaths_;
    bool closed_;

    std::string relativeStatePath_;
    std::string tempPath_;
    std::string finalPath_;

    std::ostream* outStream_;
};

#endif // OMNISTREAM_FS_CHECKPOINT_STATE_OUTPUT_STREAM_H
