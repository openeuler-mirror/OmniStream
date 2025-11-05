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
#ifndef OMNISTREAM_STATESNAPSHOTCONTEXTSYNCHRONOUSIMPL
#define OMNISTREAM_STATESNAPSHOTCONTEXTSYNCHRONOUSIMPL

#include <future>

#include "KeyGroupRange.h"
#include "CheckpointStreamFactory.h"
#include "SnapshotResult.h"
#include "KeyedStateHandle.h"
#include "OperatorStateHandle.h"
#include "KeyedStateCheckpointOutputStream.h"

class StateSnapshotContextSynchronousImpl {
public:
    StateSnapshotContextSynchronousImpl(
        long checkpointId,
        long checkpointTimestamp,
        CheckpointStreamFactory *streamFactory,
        KeyGroupRange *keyGroupRange);
    KeyedStateCheckpointOutputStream *getRawKeyedOperatorStateOutput();

    long getCheckpointId();
    long getCheckpointTimestamp();

    std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> getKeyedStateStreamFuture();
    std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> getOperatorStateStreamFuture();
    void closeExceptionally();

protected:
    std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> keyedStateCheckpointClosingFuture;
    std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> operatorStateCheckpointClosingFuture;

private:
    long checkpointId_;
    long checkpointTimestamp_;
    CheckpointStreamFactory *streamFactory_;
    KeyGroupRange *keyGroupRange_;
};

#endif // OMNISTREAM_STATESNAPSHOTCONTEXTSYNCHRONOUSIMPL