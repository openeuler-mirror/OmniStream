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
#ifndef OMNISTREAM_OPERATORSTATEBACKEND
#define OMNISTREAM_OPERATORSTATEBACKEND

#include "SnapshotResult.h"
#include "OperatorStateHandle.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "CheckpointStreamFactory.h"

class OperatorStateBackend {
public:
    std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> snapshot(
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory *streamFactory,
        CheckpointOptions *checkpointOptions)
    {
        // TTODO
        return nullptr;
    };
};

#endif // OMNISTREAM_OPERATORSTATEBACKEND