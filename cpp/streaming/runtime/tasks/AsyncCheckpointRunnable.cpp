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
#include "AsyncCheckpointRunnable.h"
#include <chrono>
#include <thread>
using namespace std::chrono;


bool AsyncCheckpointRunnable::IsRunning() const
{
    return asyncCheckpointState.load() == AsyncCheckpointState::RUNNING;
}

void AsyncCheckpointRunnable::Run()
{
    auto asyncStartNanos = std::chrono::steady_clock::now().time_since_epoch().count();
    long asyncStartDelayMillis = (asyncStartNanos - asyncConstructionNanos) / 1000000;
    LOG(taskName + " - started executing asynchronous part of checkpoint " +
        std::to_string(checkpointMetaData.GetCheckpointId()) +
        ". Asynchronous start delay: " + std::to_string(asyncStartDelayMillis) + " ms");
    FileSystemSafetyNet::initializeSafetyNetForThread();
    SnapshotsFinalizeResult *snapshotFinalizeResult = nullptr;
    try {
        snapshotFinalizeResult = isTaskDeployedAsFinished ?
            FinalizedFinishedSnapshots() : FinalizeNonFinishedSnapshots();
        long asyncEndNanos = std::chrono::steady_clock::now().time_since_epoch().count();
        long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1000000;
        checkpointMetric.SetBytesPersistedDuringAlignment(
            snapshotFinalizeResult->bytesPersistedDuringAlignment);
        checkpointMetric.SetAsyncDurationMillis(asyncStartDelayMillis);
        AsyncCheckpointState expected = AsyncCheckpointState::RUNNING;
        if (asyncCheckpointState.compare_exchange_strong(expected, AsyncCheckpointState::COMPLETED)) {
            ReportCompletedSnapshotStates(
                snapshotFinalizeResult->jobManagerTaskOperatorSubtaskStates,
                snapshotFinalizeResult->localTaskOperatorSubtaskStates,
                asyncDurationMillis);
        } else {
            LOG("asyncCheckpointState is not COMPLETED.");
        }
        finishedFuture.Complete();
    }
    catch (std::exception& e) {
        LOG(std::string("savepoint: AsyncCheckpointRunnable error ")+e.what());
        std::this_thread::sleep_for(100ms);
        HandleExecutionException(std::current_exception());
    }
    (*consumer)(this);
    FileSystemSafetyNet::closeSafetyNetAndGuardedResourcesForThread();
    delete snapshotFinalizeResult;
}

SnapshotsFinalizeResult *AsyncCheckpointRunnable::FinalizeNonFinishedSnapshots()
{
    LOG(">>>>>>> start FinalizeNonFinishedSnapshots")
    auto jobManagerTaskOperatorSubtaskStates =
        std::make_shared<TaskStateSnapshot>(operatorSnapshotsInProgress->size(), isTaskFinished);
    auto localTaskOperatorSubtaskStates =
        std::make_shared<TaskStateSnapshot>(operatorSnapshotsInProgress->size(), isTaskFinished);

    long bytesPersistedDuringAlignment = 0;
    for (auto entry : *operatorSnapshotsInProgress) {
        auto operatorID = entry.first;
        OperatorSnapshotFutures *snapshotInProgress = entry.second;

        auto finalizedSnapshot = std::make_shared<OperatorSnapshotFinalizer>(snapshotInProgress);

        jobManagerTaskOperatorSubtaskStates->PutSubtaskStateByOperatorID(
            operatorID,
            finalizedSnapshot->getJobManagerOwnedState()
        );
        localTaskOperatorSubtaskStates->PutSubtaskStateByOperatorID(
            operatorID,
            finalizedSnapshot->getTaskLocalState()
        );

        bytesPersistedDuringAlignment += finalizedSnapshot
            ->getJobManagerOwnedState()->getResultSubpartitionState().GetStateSize();
        bytesPersistedDuringAlignment += finalizedSnapshot
            ->getJobManagerOwnedState()->getInputChannelState().GetStateSize();
    }
    LOG(">>>>>>> end FinalizeNonFinishedSnapshots")
    return new SnapshotsFinalizeResult(
        jobManagerTaskOperatorSubtaskStates,
        localTaskOperatorSubtaskStates,
        bytesPersistedDuringAlignment
    );
}

SnapshotsFinalizeResult *AsyncCheckpointRunnable::FinalizedFinishedSnapshots()
{
    LOG(">>>>>>>>>")
    for (auto entry : *operatorSnapshotsInProgress) {
        auto snapshotInProgress = entry.second;
        snapshotInProgress->getInputChannelStateFuture()->get();
        snapshotInProgress->getResultSubpartitionStateFuture()->get();
    }
    return new SnapshotsFinalizeResult(
        TaskStateSnapshot::finishedOnRestore,
        TaskStateSnapshot::finishedOnRestore,
        0L
    );
}

void AsyncCheckpointRunnable::ReportCompletedSnapshotStates(std::shared_ptr<TaskStateSnapshot> acknowledgedTaskStateSnapshot,
    std::shared_ptr<TaskStateSnapshot> localTaskStateSnapshot, long asyncDurationMillis)
{
    LOG(">>>>>>> start ReportCompletedSnapshotStates")
    bool hasAckState = acknowledgedTaskStateSnapshot->HasState();
    bool hasLocalState = localTaskStateSnapshot->HasState();
    if (!(hasAckState || !hasLocalState)) {
        THROW_LOGIC_EXCEPTION(
            "Found cached state but no corresponding primary state is reported to the job manager."
        )
    }

    auto checkpointedSize = acknowledgedTaskStateSnapshot->GetCheckpointedSize();
    auto stateSize = acknowledgedTaskStateSnapshot->GetStateSize();
    auto checkpointMetrics = checkpointMetric
            .SetBytesPersistedOfThisCheckpoint(checkpointedSize)
            ->SetTotalBytesPersisted(stateSize)
            ->Build();
    taskEnvironment->getTaskStateManager()->ReportTaskStateSnapshotsV2(
        &checkpointMetaData,
        checkpointMetrics,
        hasAckState ? acknowledgedTaskStateSnapshot : nullptr,
        hasLocalState ? localTaskStateSnapshot : nullptr);
    LOG(">>>>>>> end ReportCompletedSnapshotStates")
    delete checkpointMetrics;
}

void AsyncCheckpointRunnable::HandleExecutionException(std::__exception_ptr::exception_ptr e)
{
    bool didCleanup = false;
    auto currentState = asyncCheckpointState.load();
    while (currentState != AsyncCheckpointState::DSICARDED) {
        if (asyncCheckpointState.compare_exchange_strong(currentState, AsyncCheckpointState::DSICARDED)) {
            didCleanup = true;

            try {
                Cleanup();
            }
            catch (...) {
                // Do nothing
            }

            if (isTaskRunning->get()) {
            } else {
                LOG("Ignore decline of checkpoint " +
                    std::to_string(checkpointMetaData.GetCheckpointId()) +
                    " as task is not running anymore."
                );
            }
            currentState = AsyncCheckpointState::DSICARDED;
        } else {
            currentState = asyncCheckpointState.load();
        }
    }

    if (!didCleanup) {
        LOG("Caught followup exception from a failed checkpoint thread. This can be ignored.");
    }
}

std::pair<long, long> AsyncCheckpointRunnable::Cleanup()
{
    long stateSize = 0;
    long checkpointedSize = 0;
    std::exception_ptr firstException = nullptr;

    if (operatorSnapshotsInProgress) {
        for (auto &entry : *operatorSnapshotsInProgress) {
            OperatorSnapshotFutures *operatorSnapshotResult = entry.second;
            if (operatorSnapshotResult != nullptr) {
                try {
                    auto tuple2 = operatorSnapshotResult->cancel();
                    stateSize += tuple2.first;
                    checkpointedSize += tuple2.second;
                }
                catch (...) {
                    if (!firstException) {
                        firstException = std::current_exception();
                    }
                }
            }
        }
    }

    if (firstException) {
        std::rethrow_exception(firstException);
    }
    return std::make_pair(stateSize, checkpointedSize);
}

long AsyncCheckpointRunnable::GetCheckpointId() const
{
    return checkpointMetaData.GetCheckpointId();
}

void AsyncCheckpointRunnable::Close()
{
    AsyncCheckpointState expected = AsyncCheckpointState::RUNNING;
    if (asyncCheckpointState.compare_exchange_strong(expected, AsyncCheckpointState::DSICARDED)) {
        try {
            auto tuple = Cleanup();
            ReportAbortedSnapshotStats(tuple.first, tuple.second);
        } catch (const std::exception &cleanupException) {
            LOG("Could not properly clean up the async checkpoint runnable.");
        }
    } else {
        LogFailedCleanupAttempt();
    }
}

void AsyncCheckpointRunnable::ReportAbortedSnapshotStats(long stateSize, long checkpointedSize)
{
    CheckpointMetrics *metrics = checkpointMetric
        .SetTotalBytesPersisted(stateSize)
        ->SetBytesPersistedOfThisCheckpoint(checkpointedSize)
        ->BuildIncomplete();
    taskEnvironment->getTaskStateManager()->ReportIncompleteTaskStateSnapshots(&checkpointMetaData, metrics);
}

void AsyncCheckpointRunnable::LogFailedCleanupAttempt()
{
    LOG(taskName + " - asynchronous checkpointing operation for checkpoint " +
        std::to_string(checkpointMetaData.GetCheckpointId()) +
        " has already been completed. Thus, the state handles are not cleaned up."
    );
}

bool AsyncCheckpointRunnable::IsFinished() const
{
    return finishedFuture.IsDone();  // or `is_completed()` depending on your CompletableFutureV2 implementation
}
