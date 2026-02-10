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
#ifndef OMNISTREAM_CHECKPOINTABLEKEYEDSTATEBACKEND
#define OMNISTREAM_CHECKPOINTABLEKEYEDSTATEBACKEND

#include "KeyedStateBackend.h"
#include "KeyGroupRange.h"
#include "CheckpointStreamFactory.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "state/SavepointResources.h"
#include "SnapshotResult.h"
#include "KeyedStateHandle.h"
#include <future>

template <typename K>
class CheckpointableKeyedStateBackend : public KeyedStateBackend<K> {
public:
    virtual KeyGroupRange *getKeyGroupRange() = 0;
    virtual std::shared_ptr<std::packaged_task<std::shared_ptr<SnapshotResult<KeyedStateHandle>>()>> snapshot(
        long checkpointId,
        long timestamp,
        CheckpointStreamFactory *streamFactory,
        CheckpointOptions *checkpointOptions) = 0;

    ~CheckpointableKeyedStateBackend() = default;

    virtual std::shared_ptr<SavepointResources> savepoint() = 0;
};

#endif // OMNISTREAM_CHECKPOINTABLEKEYEDSTATEBACKEND