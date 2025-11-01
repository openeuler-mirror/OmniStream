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
#include "StateSnapshotContextSynchronousImpl.h"

StateSnapshotContextSynchronousImpl::StateSnapshotContextSynchronousImpl(
    long checkpointId,
    long checkpointTimestamp,
    CheckpointStreamFactory *streamFactory,
    KeyGroupRange *keyGroupRange)
    : checkpointId_(checkpointId),
      checkpointTimestamp_(checkpointTimestamp),
      streamFactory_(streamFactory),
      keyGroupRange_(keyGroupRange)
{
}

KeyedStateCheckpointOutputStream *StateSnapshotContextSynchronousImpl::getRawKeyedOperatorStateOutput()
{
    // TTODO
    return nullptr;
}

long StateSnapshotContextSynchronousImpl::getCheckpointId()
{
    return checkpointId_;
}

long StateSnapshotContextSynchronousImpl::getCheckpointTimestamp()
{
    return checkpointTimestamp_;
}

std::shared_ptr<std::packaged_task<SnapshotResult<KeyedStateHandle>*()>> StateSnapshotContextSynchronousImpl::getKeyedStateStreamFuture()
{
    if (keyedStateCheckpointClosingFuture == nullptr) {
        // TTODO
    }
    return keyedStateCheckpointClosingFuture;
}

std::shared_ptr<std::packaged_task<SnapshotResult<OperatorStateHandle>>> StateSnapshotContextSynchronousImpl::getOperatorStateStreamFuture()
{
    if (operatorStateCheckpointClosingFuture == nullptr) {
        // TTODO
    }
    return operatorStateCheckpointClosingFuture;
}

void StateSnapshotContextSynchronousImpl::closeExceptionally()
{
    // TTODO
}
