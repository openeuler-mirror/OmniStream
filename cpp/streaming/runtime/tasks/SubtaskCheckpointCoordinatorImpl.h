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

#ifndef FLINK_TNEL_SUBTASKCHECKPOINTCOORDINATORIMPL_H
#define FLINK_TNEL_SUBTASKCHECKPOINTCOORDINATORIMPL_H
#include <unordered_map>
#include <set>
#include <chrono>
#include <memory>
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "runtime/checkpoint/channel/ChannelStateWriterImpl.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/CheckpointStorageWorkerView.h"
#include "mailbox/StreamTaskActionExecutor.h"
#include "streaming/runtime/io/checkpointing/BarrierAlignmentUtil.h"
#include "OperatorChain.h"
#include "streaming/api/operators/OperatorSnapshotFutures.h"
#include "runtime/state/CheckpointStreamFactory.h"
#include "streaming/runtime/tasks/AsyncCheckpointRunnable.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/io/network/api/CancelCheckpointMarker.h"
#include "runtime/jobgraph/OperatorID.h"
#include "runtime/jobgraph/JobVertexID.h"
#include "core/utils/function/Supplier.h"
#include "core/utils/threads/ExecutorService.h"
#include "runtime/execution/OmniEnvironment.h"
#include "SubtaskCheckpointCoordinator.h"
namespace omnistream::runtime {
    class SubtaskCheckpointCoordinatorImpl : public omnistream::SubtaskCheckpointCoordinator {
    public:
        SubtaskCheckpointCoordinatorImpl(
                CheckpointStorageWorkerView *checkpointStorage,
                std::string taskName,
                std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
                std::shared_ptr<omnistream::EnvironmentV2> env,
                bool unalignedCheckpointEnabled,
                bool enableCheckpointAfterTasksFinished,
                std::function<CompletableFutureV2<void> *(ChannelStateWriter *, long)> *prepareInputSnapshot,
                BarrierAlignmentUtil::DelayableTimer<std::function<void()>> *registerTimer
        ) : SubtaskCheckpointCoordinatorImpl(
                nullptr,
                checkpointStorage,
                taskName,
                actionExecutor,
                env,
                unalignedCheckpointEnabled,
                enableCheckpointAfterTasksFinished,
                prepareInputSnapshot,
                defaultMaxRecordAbortedCheckpoints,
                registerTimer
        ) {};

        SubtaskCheckpointCoordinatorImpl(
                omnistream::CheckpointStorage *checkpointStorage,
                CheckpointStorageWorkerView *checkpointStorageView,
                std::string taskName,
                std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
                std::shared_ptr<omnistream::EnvironmentV2> env,
                bool unalignedCheckpointEnabled,
                bool enableCheckpointAfterTasksFinished,
                std::function<CompletableFutureV2<void> *(ChannelStateWriter *, long)> *prepareInputSnapshot,
                int maxRecordAbortedCheckpoints,
                BarrierAlignmentUtil::DelayableTimer<std::function<void()>> *registerTimer
        ) : SubtaskCheckpointCoordinatorImpl(
                checkpointStorageView,
                taskName,
                actionExecutor,
                env,
                prepareInputSnapshot,
                maxRecordAbortedCheckpoints,
                unalignedCheckpointEnabled ?
                openChannelStateWriter(taskName, checkpointStorage, env) :
                NoOpChannelStateWriter::noOp,
                enableCheckpointAfterTasksFinished,
                registerTimer
        )
        {
            asyncOperationsThreadPool = std::make_unique<ExecutorService>(4);
        };

        SubtaskCheckpointCoordinatorImpl(
                CheckpointStorageWorkerView *checkpointStorage,
                std::string taskName,
                std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor,
                std::shared_ptr<omnistream::EnvironmentV2> env,
                std::function<CompletableFutureV2<void> *(ChannelStateWriter *, long)> *prepareInputSnapshot,
                int maxRecordAbortedCheckpoints,
                ChannelStateWriter *channelStateWriter,
                bool enableCheckpointAfterTasksFinished,
                BarrierAlignmentUtil::DelayableTimer<std::function<void()>> *registerTimer
        );

        ~SubtaskCheckpointCoordinatorImpl();

        void checkpointState(
                CheckpointMetaData *metadata,
                CheckpointOptions *options,
                CheckpointMetricsBuilder *metrics,
                omnistream::OperatorChainV2 *operatorChain,
                bool isTaskFinished,
                omnistream::Supplier<bool> *isRunning
        ) override;

        void InitInputsCheckpoint(long checkpointId, CheckpointOptions *options) override;

        enum class NotifyCheckpointOperation {
            COMPLETE,
            ABORT,
            SUBSUME
        };

        void Close();

        void Cancel();

        void notifyCheckpointComplete(
                long checkpointId,
                omnistream::OperatorChainV2 *operatorChain,
                omnistream::Supplier<bool> *isRunning);

        void notifyCheckpointAborted(
                long checkpointId,
                omnistream::OperatorChainV2 *operatorChain,
                omnistream::Supplier<bool> *isRunning);

        const unordered_map<long, AsyncCheckpointRunnable *> &GetCheckpoints() const;

        void notifyCheckpointSubsumed(
                long checkpointId,
                omnistream::OperatorChainV2 *operatorChain,
                omnistream::Supplier<bool> *isRunning);

        CheckpointStorageWorkerView *getCheckpointStorage();

        ChannelStateWriter *getChannelStateWriter();

    private:
        class LimitedSizeSet : public std::set<long> {
        public:
            explicit LimitedSizeSet(size_t maxSize) : maxSize(maxSize) {}

            std::pair<iterator, bool> insert(const long &value)
            {
                if (this->size() >= maxSize) {
                    this->erase(this->begin());
                }
                return std::set<long>::insert(value);
            };

        private:
            size_t maxSize;
        };

        class CachingCheckpointStorageWorkerView : public CheckpointStorageWorkerView {
        public:
            CachingCheckpointStorageWorkerView() = default;

            explicit CachingCheckpointStorageWorkerView(CheckpointStorageWorkerView *delegate)
                : delegate(delegate) {}

            void clearCacheFor(long checkpointId)
            {
                cache.erase(checkpointId);
            }

            CheckpointStreamFactory *resolveCheckpointStorageLocation(
                    int64_t checkpointId, CheckpointStorageLocationReference &reference) override;

            CheckpointStateOutputStream *createTaskOwnedStateStream() override;

            CheckpointStateToolset *createTaskOwnedCheckpointStateToolset() override;

        private:
            std::unordered_map<long, CheckpointStreamFactory *> cache;
            CheckpointStorageWorkerView *delegate;
        };

        std::set<long> createAbortedCheckpointIds(int maxRecordAbortedCheckpoints);

        bool CheckAndClearAbortedStatus(long checkpointId);

        static void logCheckpointProcessingDelay(CheckpointMetaData *metadata);

        void registerAlignmentTimer(
                long checkpointId,
                omnistream::OperatorChainV2 *operatorChain,
                CheckpointBarrier *checkpointBarrier
        );

        void CancelAlignmentTimer();

        void PrepareInflightDataSnapshot(long checkpointId);

        bool CancelAsyncCheckpointRunnable(long checkpointId);

        void RegisterAsyncCheckpointRunnable(int64_t checkpointId,
                                             AsyncCheckpointRunnable *asyncCheckpointRunnable);

        bool UnregisterAsyncCheckpointRunnable(long checkpointId);

        bool takeSnapshotSync(
                std::unordered_map<OperatorID, OperatorSnapshotFutures *> *operatorSnapshotsInProgress,
                CheckpointMetaData *checkpointMetaData,
                CheckpointMetricsBuilder *checkpointMetrics,
                CheckpointOptions *checkpointOptions,
                omnistream::OperatorChainV2 *operatorChain,
                omnistream::Supplier<bool> *isRunning
        );

        void cleanup(
                std::unordered_map<OperatorID, OperatorSnapshotFutures *> *operatorSnapshotsInProgress,
                CheckpointMetaData *metadata,
                CheckpointMetricsBuilder *metrics,
                std::exception ex
        );

        void CloseQuietly(AsyncCheckpointRunnable *runnable)
        {
            try {
                if (runnable != nullptr) {
                    runnable->Close();
                }
            } catch (const std::exception &e) {
                LogError("Exception while closing runnable: %s", e.what());
            }
        }

        void finishAndReportAsync(
                std::unordered_map<OperatorID, OperatorSnapshotFutures *> *operatorSnapshotsInProgress,
                CheckpointMetaData *metadata,
                CheckpointMetricsBuilder *metrics,
                bool istaskDeployedAsFinished,
                bool isTaskFinished,
                omnistream::Supplier<bool> *isRunning
        );

        static ChannelStateWriter *openChannelStateWriter(
                std::string taskName,
                omnistream::CheckpointStorage *checkpointStorage,
                std::shared_ptr<omnistream::EnvironmentV2> env
        );

        void notifyCheckpoint(
                long checkpointId,
                omnistream::OperatorChainV2 *operatorChain,
                omnistream::Supplier<bool> *isRunning,
                NotifyCheckpointOperation notifyCheckpointOperation
        );
        CachingCheckpointStorageWorkerView *checkpointStorage;
        std::string taskName;
        std::shared_ptr<omnistream::StreamTaskActionExecutor> actionExecutor;
        std::shared_ptr<omnistream::EnvironmentV2> env;
        std::function<CompletableFutureV2<void> *(ChannelStateWriter *, long)> *prepareInputSnapshot;
        ChannelStateWriter *channelStateWriter;
        std::set<long> abortedCheckpointIds;
        bool enableCheckpointAfterTasksFinished;
        BarrierAlignmentUtil::DelayableTimer<std::function<void()>> *registerTimer;
        long lastCheckpointId;

        static const int defaultMaxRecordAbortedCheckpoints = 128;
        static const int checkpointExecutionDelayLogThresholdMs = 30000;
        long alignmentCheckpointId;
        BarrierAlignmentUtil::Cancellable *alignmentTimer = nullptr;
        std::mutex mutexLock;
        std::unique_ptr<ExecutorService> asyncOperationsThreadPool;
        bool closed = false;
        std::unordered_map<long, AsyncCheckpointRunnable *> checkpoints;
    };
}
#endif // FLINK_TNEL_SUBTASKCHECKPOINTCOORDINATORIMPL_H
