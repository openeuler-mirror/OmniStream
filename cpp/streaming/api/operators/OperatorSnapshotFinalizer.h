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
#ifndef OMNISTREAM_OPERATORSNAPSHOTFINALIZER_H
#define OMNISTREAM_OPERATORSNAPSHOTFINALIZER_H
#include "OperatorSnapshotFutures.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/checkpoint/OperatorSubtaskState.h"
#include "core/utils/threads/FutureUtils.h"
#include "runtime/checkpoint/StateObjectCollection.h"

// Placeholder class
class OperatorSnapshotFinalizer {
public:
    explicit OperatorSnapshotFinalizer(OperatorSnapshotFutures *snapshotFutures)
    {
        LOG(">>>>>>> start OperatorSnapshotFinalizer")
        auto keyedStateManaged = FutureUtils::runIfNotDoneAndGet(snapshotFutures->getKeyedStateManagedFuture());
        auto KeyedStateRaw = FutureUtils::runIfNotDoneAndGet(snapshotFutures->getKeyedStateRawFuture());

        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> jobManagerOwnedManaged;
        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> taskLocalManaged;
        if (keyedStateManaged) {
            jobManagerOwnedManaged = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(
                keyedStateManaged->GetJobManagerOwnedSnapshot());
            taskLocalManaged = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(
                keyedStateManaged->GetTaskLocalSnapshot());
        } else {
            jobManagerOwnedManaged = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
            taskLocalManaged = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
        }
        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> jobManagerOwnedRaw;
        std::shared_ptr<StateObjectCollection<KeyedStateHandle>> taskLocalRaw;
        if (KeyedStateRaw) {
            jobManagerOwnedRaw = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(
                KeyedStateRaw->GetJobManagerOwnedSnapshot());
            taskLocalRaw = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(
                KeyedStateRaw->GetTaskLocalSnapshot());
        } else {
            jobManagerOwnedRaw = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
            taskLocalRaw = StateObjectCollection<KeyedStateHandle>::SingletonOrEmpty(nullptr);
        }

        JobManagerOwnedState = std::make_shared<OperatorSubtaskState>(
            *StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr),
            *StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr),
            *jobManagerOwnedManaged,
            *jobManagerOwnedRaw,
            *StateObjectCollection<InputChannelStateHandle>::EmptyIfNull(nullptr),
            *StateObjectCollection<ResultSubpartitionStateHandle>::EmptyIfNull(nullptr));
        taskLocalState = std::make_shared<OperatorSubtaskState>(
            *StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr),
            *StateObjectCollection<OperatorStateHandle>::SingletonOrEmpty(nullptr),
            *taskLocalManaged,
            *taskLocalRaw,
            *StateObjectCollection<InputChannelStateHandle>::EmptyIfNull(nullptr),
            *StateObjectCollection<ResultSubpartitionStateHandle>::EmptyIfNull(nullptr));
        LOG(">>>>>>> end OperatorSnapshotFinalizer")
    };

    [[nodiscard]] std::shared_ptr<OperatorSubtaskState> getTaskLocalState() const
    {
        return taskLocalState;
    }
    [[nodiscard]] std::shared_ptr<OperatorSubtaskState> getJobManagerOwnedState() const
    {
        return JobManagerOwnedState;
    }
private:
    std::shared_ptr<OperatorSubtaskState> JobManagerOwnedState;
    std::shared_ptr<OperatorSubtaskState> taskLocalState;
};

#endif // OMNISTREAM_OPERATORSNAPSHOTFINALIZER_H