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
#include <algorithm>
#include "SubtaskCheckpointCoordinatorImpl.h"
#include "runtime/io/network/api/CancelCheckpointMarker.h"

namespace omnistream::runtime {
    std::set<long> SubtaskCheckpointCoordinatorImpl::createAbortedCheckpointIds(int maxRecordAbortedCheckpoints)
    {
        return LimitedSizeSet(static_cast<size_t>(maxRecordAbortedCheckpoints));
    }

    bool SubtaskCheckpointCoordinatorImpl::CheckAndClearAbortedStatus(long checkpointId)
    {
        if (abortedCheckpointIds.find(checkpointId) != abortedCheckpointIds.end()) {
            abortedCheckpointIds.erase(checkpointId);
            return true;
        }
        return false;
    }

    void SubtaskCheckpointCoordinatorImpl::logCheckpointProcessingDelay(CheckpointMetaData *metadata)
    {
        long delay =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count() - metadata->GetReceiveTimestamp();
        if (delay >= checkpointExecutionDelayLogThresholdMs) {
            LOG("Time from receiving all checkpoint barriers/RPC to executing it exceeded threshold: " << delay << "ms");
        }
    }

    void SubtaskCheckpointCoordinatorImpl::registerAlignmentTimer(
        long checkpointId,
        omnistream::OperatorChainV2 *operatorChain,
        CheckpointBarrier *checkpointBarrier)
    {
        CancelAlignmentTimer();
        if (!checkpointBarrier->GetCheckpointOptions()->IsTimeoutable()) {
            return;
        }
        long timerDelay = BarrierAlignmentUtil::
        getTimerDelay(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count(),
            *checkpointBarrier);

        alignmentTimer = registerTimer->RegisterTask(
            std::function([this, operatorChain, checkpointId]() {
                try {
                    operatorChain->AlignedBarrierTimeout(checkpointId);
                } catch (const std::exception &e) {
                    LOG("Exception during alignment timer execution: " + std::string(e.what()));
                }
                this->alignmentTimer = nullptr;
            }),
            std::chrono::milliseconds(timerDelay));

        alignmentCheckpointId = checkpointId;
    }

    void SubtaskCheckpointCoordinatorImpl::CancelAlignmentTimer()
    {
        if (alignmentTimer) {
            alignmentTimer->Cancel();
            alignmentTimer = nullptr;
        }
    }

    void SubtaskCheckpointCoordinatorImpl::PrepareInflightDataSnapshot(long checkpointId)
    {
        auto future = (*prepareInputSnapshot)(channelStateWriter, checkpointId);
        future->ThenRun([this, checkpointId, future]() mutable {
            try {
                future->Get();
                channelStateWriter->FinishInput(checkpointId);
            } catch (...) {
                auto ex = std::current_exception();
                channelStateWriter->Abort(checkpointId, ex, false);
            }
        });
    }

    bool SubtaskCheckpointCoordinatorImpl::CancelAsyncCheckpointRunnable(long checkpointId)
    {
        lock_guard<std::mutex> lock(mutexLock);
        auto it = checkpoints.find(checkpointId);
        if (it != checkpoints.end()) {
            it->second->Close();
            checkpoints.erase(it);
            return true;
        }
        return false;
    }

    bool SubtaskCheckpointCoordinatorImpl::takeSnapshotSync(
        std::unordered_map<OperatorID, OperatorSnapshotFutures *> *operatorSnapshotsInProgress,
        CheckpointMetaData *checkpointMetaData,
        CheckpointMetricsBuilder *checkpointMetrics,
        CheckpointOptions *checkpointOptions,
        omnistream::OperatorChainV2 *operatorChain,
        std::shared_ptr<omnistream::Supplier<bool>> isRunning)
    {
        LOG(">>>>>>>>>")
        if (operatorChain->IsClosed()) {
            THROW_RUNTIME_ERROR("OperatorChain and Task should never be closed at this point");
        }

        long checkpointId = checkpointMetaData->GetCheckpointId();
        long started = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch())
                .count();
        auto channelStateWriteResult = checkpointOptions->NeedsChannelState()
                                       ? channelStateWriter->GetAndRemoveWriteResult(checkpointId)
                                       : ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();

        CheckpointStreamFactory *storage =
                checkpointStorage->resolveCheckpointStorageLocation(
                    checkpointId,
                    checkpointOptions->GetTargetLocation());

        try {
            operatorChain->SnapshotState(
                *operatorSnapshotsInProgress,
                *checkpointMetaData,
                checkpointOptions,
                isRunning,
                channelStateWriteResult,
                storage,
                env->getTaskStateManager()->getOmniTaskBridge());
        } catch (...) {
            checkpointStorage->clearCacheFor(checkpointId);
        }

        checkpointStorage->clearCacheFor(checkpointId);

        constexpr int nanoToMillis = 1000000;

        checkpointMetrics->SetSyncDurationMillis(
            (std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now().time_since_epoch())
                        .count() -
                started) /
            nanoToMillis);
        return true;
    }

    void SubtaskCheckpointCoordinatorImpl::cleanup(
        std::unordered_map<OperatorID, OperatorSnapshotFutures *> *operatorSnapshotsInProgress,
        CheckpointMetaData *metadata,
        CheckpointMetricsBuilder *operatorChain,
        std::exception ex)
    {
        channelStateWriter->Abort(metadata->GetCheckpointId(), std::make_exception_ptr(ex), true);
        for (auto &entry: *operatorSnapshotsInProgress) {
            OperatorSnapshotFutures *operatorSnapshotResult = entry.second;
            if (operatorSnapshotResult) {
                try {
                    operatorSnapshotResult->cancel();
                } catch (const std::exception &e) {
                    LOG("Could not poperly cancel an operator snapshot result. " + std::string(e.what()));
                }
            }
        }
    }

    void SubtaskCheckpointCoordinatorImpl::finishAndReportAsync(
        std::unordered_map<OperatorID, OperatorSnapshotFutures *> *operatorSnapshotsInProgress,
        CheckpointMetaData *metadata,
        CheckpointMetricsBuilder *metrics,
        bool istaskDeployedAsFinished,
        bool isTaskFinished,
        std::shared_ptr<omnistream::Supplier<bool>> isRunning,
        CheckpointOptions *options)
    {
        LOG(">>>>>> isTaskDeployedAsFinished " << istaskDeployedAsFinished << " isTaskFinished " << isTaskFinished);
        AsyncCheckpointRunnable *asyncCheckpointRunnable = new AsyncCheckpointRunnable(
                operatorSnapshotsInProgress,
                *metadata,
                *metrics,
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count(),
                taskName,
                new std::function([this](AsyncCheckpointRunnable *asyncCheckpointRunnable) {
                    this->UnregisterAsyncCheckpointRunnable(asyncCheckpointRunnable->GetCheckpointId());
                }),
                env,
                new std::function([](std::string taskName, std::exception e) {
                    LOG("Async checkpoint exception in task " + taskName + ": " + std::string(e.what()));
                }),
                istaskDeployedAsFinished,
                isTaskFinished,
                isRunning);
        RegisterAsyncCheckpointRunnable(asyncCheckpointRunnable->GetCheckpointId(), asyncCheckpointRunnable);
        asyncOperationsThreadPool->Execute([asyncCheckpointRunnable,
            operatorSnapshotsInProgress,
            metadata,
            metrics,
            options]() {
            try {
                asyncCheckpointRunnable->Run();
            } catch (const std::exception &e) {
                LogError("Exception in async checkpoint: %s", e.what());
            }
            delete asyncCheckpointRunnable;
            delete operatorSnapshotsInProgress;
            delete metadata;
            delete metrics;
            delete options;
        });
        LOG(">>>>> Done")
    }

    void SubtaskCheckpointCoordinatorImpl::RegisterAsyncCheckpointRunnable(
        int64_t checkpointId,
        AsyncCheckpointRunnable *asyncCheckpointRunnable)
    {
        std::lock_guard<std::mutex> guard(mutexLock);
        if (closed) {
            std::cerr << "Cannot register runnable; coordinator is already closed. Closing runnable." << std::endl;
            CloseQuietly(asyncCheckpointRunnable);
            if (checkpoints.count(checkpointId) == 0) {
                throw std::runtime_error(
                    "SubtaskCheckpointCoordinator was closed without releasing asyncCheckpointRunnable for checkpoint " +
                    std::to_string(checkpointId));
            }
        } else if (checkpoints.count(checkpointId) > 0) {
            CloseQuietly(asyncCheckpointRunnable);
            std::stringstream ss;
            ss << "Cannot register runnable; async checkpoint " << checkpointId << " already registered.";
            throw std::runtime_error(ss.str());
        } else {
            LOG(">>>>>> checkpointId " << checkpointId << " AsyncCheckpointRunnable Registered")
            checkpoints[checkpointId] = asyncCheckpointRunnable;
        }
    }

    std::shared_ptr<ChannelStateWriter> SubtaskCheckpointCoordinatorImpl::openChannelStateWriter(
        std::string taskName, std::shared_ptr<omnistream::CheckpointStorage> checkpointStorage,
        std::shared_ptr<omnistream::CheckpointStorageWorkerView> streamFactoryResolver,
        std::shared_ptr<omnistream::EnvironmentV2> env)
    {
        // JobIDPOD seems to be similar to JobVertexID. Remove one, then replace the temp JobVertexID with:
        //      env->taskConfiguration().jobConfiguration().getJobId()

        std::shared_ptr<ChannelStateWriterImpl> writer = std::make_shared<omnistream::ChannelStateWriterImpl>(omnistream::JobVertexID(0, 0),
                                                      taskName,
                                                      env->taskConfiguration().getIndexOfSubtask(),
                                                      checkpointStorage,
                                                      streamFactoryResolver);
        writer->open();
        return writer;
    }

    bool SubtaskCheckpointCoordinatorImpl::UnregisterAsyncCheckpointRunnable(long checkpointId)
    {
        std::lock_guard<std::mutex> lock(mutexLock);
        return checkpoints.erase(checkpointId) > 0;
    }

    void SubtaskCheckpointCoordinatorImpl::checkpointState(
        CheckpointMetaData *metadata,
        CheckpointOptions *options,
        CheckpointMetricsBuilder *metrics,
        omnistream::OperatorChainV2 *operatorChain,
        bool isTaskFinished,
        std::shared_ptr<omnistream::Supplier<bool>> isRunning)
    {
        LOG_DEBUG(">>>>>>> isTaskFinished? " << isTaskFinished)
        if (!options || !metrics) {
            THROW_LOGIC_EXCEPTION("CheckpointOptions or CheckpointMetricsBuilder is null");
        }

        if (lastCheckpointId >= metadata->GetCheckpointId()) {
            CheckAndClearAbortedStatus(metadata->GetCheckpointId());
            return;
        }

        logCheckpointProcessingDelay(metadata);

        lastCheckpointId = metadata->GetCheckpointId();
        if (CheckAndClearAbortedStatus(metadata->GetCheckpointId())) {
            operatorChain->broadcastEvent(
                std::make_shared<omnistream::CancelCheckpointMarker>(metadata->GetCheckpointId()));
            LOG("Checkpoint " + std::to_string(metadata->GetCheckpointId()) +
                " has been notified as aborted, would not trigger any checkpoint.");
            return;
        }

        if (options->GetAlignment() == CheckpointOptions::AlignmentType::FORCED_ALIGNED) {
            options = options->WithUnalignedSupported();
            InitInputsCheckpoint(metadata->GetCheckpointId(), options);
        }

        operatorChain->PrepareSnapshotPreBarrier(metadata->GetCheckpointId());

        CheckpointBarrier *checkpointBarrier =
                new CheckpointBarrier(metadata->GetCheckpointId(), metadata->GetTimestamp(), options);

        operatorChain->broadcastEvent(std::shared_ptr<omnistream::AbstractEvent>(checkpointBarrier),
                                      options->IsUnalignedCheckpoint());

        if (options->NeedsChannelState()) {
            channelStateWriter->FinishOutput(metadata->GetCheckpointId());
        }

        std::unordered_map<OperatorID, OperatorSnapshotFutures *> *snapshotFutures =
                new std::unordered_map<OperatorID, OperatorSnapshotFutures *>();
        try {
            if (takeSnapshotSync(snapshotFutures, metadata, metrics, options, operatorChain, isRunning)) {
                LOG_DEBUG("finishAndReportAsync start lastCheckpointId: " << lastCheckpointId)
                finishAndReportAsync(snapshotFutures, metadata, metrics,
                    operatorChain->IsTaskDeployedAsFinished(), isTaskFinished, isRunning, options);
                LOG_DEBUG("finishAndReportAsync end lastCheckpointId: " << lastCheckpointId)
            } else {
                cleanup(snapshotFutures, metadata, metrics, std::runtime_error("Checkpoint declined"));
            }
        } catch (const std::exception &e) {
            LOG("Exception during checkpointing: " + std::string(e.what()));
            cleanup(snapshotFutures, metadata, metrics, e);
        }
    }

    SubtaskCheckpointCoordinatorImpl::~SubtaskCheckpointCoordinatorImpl()
    {
        if (alignmentTimer) {
            delete alignmentTimer;
        }
        if (registerTimer) {
            delete registerTimer;
        }
        if (prepareInputSnapshot) {
            delete prepareInputSnapshot;
        }
        for (auto pair: checkpoints) {
            if (pair.second) {
                delete pair.second;
            }
        }
    }

    void SubtaskCheckpointCoordinatorImpl::InitInputsCheckpoint(long checkpointId, CheckpointOptions *options)
    {
        if (options->IsUnalignedCheckpoint()) {
            channelStateWriter->Start(checkpointId, *options);
            PrepareInflightDataSnapshot(checkpointId);
        } else if (options->IsTimeoutable()) {
            channelStateWriter->Start(checkpointId, *options);
            channelStateWriter->FinishInput(checkpointId);
        }
    }

    SubtaskCheckpointCoordinatorImpl::SubtaskCheckpointCoordinatorImpl(
        std::shared_ptr<CheckpointStorageWorkerView> checkpointStorage,
        std::string taskName,
        std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
        std::shared_ptr<omnistream::EnvironmentV2> env,
        std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)> *prepareInputSnapshot,
        int maxRecordAbortedCheckpoints,
        std::shared_ptr<ChannelStateWriter> channelStateWriter,
        bool enableCheckpointAfterTasksFinished,
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>> *registerTimer)
        : checkpointStorage(make_shared<CachingCheckpointStorageWorkerView>(checkpointStorage)),
          taskName(taskName),
          actionExecutor(actionExecutor),
          env(env),
          prepareInputSnapshot(prepareInputSnapshot),
          channelStateWriter(channelStateWriter),
          abortedCheckpointIds(createAbortedCheckpointIds(maxRecordAbortedCheckpoints)),
          enableCheckpointAfterTasksFinished(enableCheckpointAfterTasksFinished),
          registerTimer(registerTimer),
          lastCheckpointId(-1) {
    }

    CheckpointStreamFactory *SubtaskCheckpointCoordinatorImpl::CachingCheckpointStorageWorkerView::resolveCheckpointStorageLocation(
        int64_t checkpointId,
        std::shared_ptr<CheckpointStorageLocationReference> reference)
    {
        auto it = cache.find(checkpointId);
        if (it != cache.end()) {
            return it->second;
        }
        try {
            CheckpointStreamFactory *factory = delegate->resolveCheckpointStorageLocation(checkpointId, reference);
            cache[checkpointId] = factory;
            return factory;
        } catch (const std::exception &e) {
            throw std::runtime_error(e.what());
        }
    }

    CheckpointStateOutputStream *SubtaskCheckpointCoordinatorImpl::CachingCheckpointStorageWorkerView::createTaskOwnedStateStream()
    {
        return delegate->createTaskOwnedStateStream();
    }

    CheckpointStateToolset *SubtaskCheckpointCoordinatorImpl::CachingCheckpointStorageWorkerView::createTaskOwnedCheckpointStateToolset()
    {
        return delegate->createTaskOwnedCheckpointStateToolset();
    }

    void SubtaskCheckpointCoordinatorImpl::notifyCheckpointComplete(
        long checkpointId,
        omnistream::OperatorChainV2 *operatorChain,
        omnistream::Supplier<bool> *isRunning)
    {
        notifyCheckpoint(checkpointId, operatorChain, isRunning, NotifyCheckpointOperation::COMPLETE);
    }

    void SubtaskCheckpointCoordinatorImpl::notifyCheckpointAborted(
        long checkpointId,
        omnistream::OperatorChainV2 *operatorChain,
        omnistream::Supplier<bool> *isRunning)
    {
        notifyCheckpoint(checkpointId, operatorChain, isRunning, NotifyCheckpointOperation::ABORT);
    }

    void SubtaskCheckpointCoordinatorImpl::notifyCheckpointSubsumed(
        long checkpointId,
        omnistream::OperatorChainV2 *operatorChain,
        omnistream::Supplier<bool> *isRunning)
    {
        notifyCheckpoint(checkpointId, operatorChain, isRunning, NotifyCheckpointOperation::SUBSUME);
    }

    void SubtaskCheckpointCoordinatorImpl::AbortCheckpointOnBarrier(
        long checkpointId,
        const std::exception_ptr& cause)
    {
        // - update lastCheckpointId
        // - prune aborted ids below lastCheckpointId
        // - record this aborted checkpoint id
        // - clear storage cache
        // - abort channel-state writer and clean up
        // - cancel any in-progress alignment timer for this checkpoint
        lastCheckpointId = std::max(lastCheckpointId, checkpointId);

        for (auto it = abortedCheckpointIds.begin(); it != abortedCheckpointIds.end();) {
            if (*it < lastCheckpointId) {
                it = abortedCheckpointIds.erase(it);
            } else {
                break;
            }
        }
        abortedCheckpointIds.insert(checkpointId);

        if (checkpointStorage) {
            checkpointStorage->clearCacheFor(checkpointId);
        }

        if (channelStateWriter) {
            channelStateWriter->Abort(checkpointId, cause, true);
        }

        try {
            if (env && env->getTaskStateManager()) {
                env->getTaskStateManager()->NotifyCheckpointAbortedV2(checkpointId);
            }
        } catch (...) {
            // Best-effort.
        }

        if (checkpointId == alignmentCheckpointId) {
            CancelAlignmentTimer();
        }
    }

    void SubtaskCheckpointCoordinatorImpl::notifyCheckpoint(
        long checkpointId,
        omnistream::OperatorChainV2 *operatorChain,
        omnistream::Supplier<bool> *isRunning,
        NotifyCheckpointOperation notifyCheckpointOperation)
    {
        std::exception_ptr previousException = nullptr;
        try {
            if (isRunning->get()) {
                if (notifyCheckpointOperation == NotifyCheckpointOperation::ABORT) {
                    bool canceled = CancelAsyncCheckpointRunnable(checkpointId);
                    if (!canceled && checkpointId > lastCheckpointId) {
                        abortedCheckpointIds.insert(checkpointId);
                    }

                    channelStateWriter->Abort(
                        checkpointId,
                        std::make_exception_ptr(std::runtime_error("checkpoint aborted via notification")),
                        false);
                }

                try {
                    switch (notifyCheckpointOperation) {
                        case NotifyCheckpointOperation::ABORT:
                            operatorChain->NotifyCheckpointAborted(checkpointId);
                            break;
                        case NotifyCheckpointOperation::COMPLETE:
                            operatorChain->NotifyCheckpointComplete(checkpointId);
                            break;
                        case NotifyCheckpointOperation::SUBSUME:
                            operatorChain->NotifyCheckpointSubsumed(checkpointId);
                            break;
                    }
                } catch (...) {
                    previousException = std::current_exception();
                }
            }
        } catch (...) {
            // No catch logic
        }

        try {
            switch (notifyCheckpointOperation) {
                case NotifyCheckpointOperation::ABORT:
                    env->getTaskStateManager()->NotifyCheckpointAbortedV2(checkpointId);
                    break;
                case NotifyCheckpointOperation::COMPLETE:
                    env->getTaskStateManager()->NotifyCheckpointCompleteV2(checkpointId);
                    break;
                default:
                    break;
            }
        } catch (...) {
            previousException = std::current_exception();
        }
    }

    std::shared_ptr<CheckpointStorageWorkerView> SubtaskCheckpointCoordinatorImpl::getCheckpointStorage()
    {
        return checkpointStorage;
    }

    std::shared_ptr<ChannelStateWriter> SubtaskCheckpointCoordinatorImpl::getChannelStateWriter()
    {
        return channelStateWriter;
    }

    void SubtaskCheckpointCoordinatorImpl::Close()
    {
        CancelAlignmentTimer();
        Cancel();
    }

    void SubtaskCheckpointCoordinatorImpl::Cancel()
    {
        std::vector<AsyncCheckpointRunnable *> asyncCheckpointRunnables;
        std::unique_lock<std::mutex> lock(mutexLock);
        if (!closed) {
            closed = true;
            for (const auto &pair: checkpoints) {
                asyncCheckpointRunnables.push_back(pair.second);
            }
            checkpoints.clear();
        }

        // Close all runnables quietly
        for (auto runnable: asyncCheckpointRunnables) {
            CloseQuietly(runnable);
        }

        try {
            if (channelStateWriter) {
            }
        } catch (const std::exception &e) {
            LogError("Failed to close channelStateWriter: %s", e.what());
        }
    }

    const unordered_map<long, AsyncCheckpointRunnable *> &SubtaskCheckpointCoordinatorImpl::GetCheckpoints() const
    {
        return checkpoints;
    }
}