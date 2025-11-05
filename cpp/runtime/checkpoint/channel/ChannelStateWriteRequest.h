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
#ifndef OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_H
#define OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_H

#include <memory>
#include <functional>
#include <atomic>
#include <vector>
#include <stdexcept>
#include "runtime/jobgraph/JobVertexID.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "runtime/partition/ResultSubpartitionInfoPOD.h"
#include "runtime/state/CheckpointStorageLocationReference.h"
#include "ChannelStateWriter.h"
#include "ChannelStateCheckpointWriter.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/buffer/ObjectBuffer.h"

namespace omnistream {

    enum class CheckpointInProgressRequestState {
        NEW,
        EXECUTING,
        COMPLETED,
        FAILED,
        CANCELLED
    };

    class ChannelStateWriteRequest {
    public:
        ChannelStateWriteRequest(JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::string &name);
        virtual ~ChannelStateWriteRequest() = default;

        JobVertexID getJobVertexID() const;
        int getSubtaskIndex() const;
        long getCheckpointId() const;
        std::string getName() const;

        virtual std::shared_ptr<CompletableFutureV2<void>> getReadyFuture();
        virtual void cancel(const std::exception_ptr &cause) = 0;
        virtual void execute(ChannelStateCheckpointWriter &writer) = 0;

        static std::unique_ptr<ChannelStateWriteRequest> completeInput(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId);

        static std::unique_ptr<ChannelStateWriteRequest> completeOutput(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId);

        static std::unique_ptr<ChannelStateWriteRequest> writeInput(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            InputChannelInfo info,
            std::vector<ObjectBuffer *> buffers);

        static std::unique_ptr<ChannelStateWriteRequest> writeOutput(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ResultSubpartitionInfoPOD info,
            std::vector<ObjectBuffer *> buffers);

        static std::unique_ptr<ChannelStateWriteRequest> writeOutputFuture(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ResultSubpartitionInfoPOD info,
            std::shared_ptr<CompletableFutureV2<std::vector<ObjectBuffer *>>> dataFuture);

        static std::unique_ptr<ChannelStateWriteRequest> start(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriter::ChannelStateWriteResult &targetResult,
            CheckpointStorageLocationReference locationReference);

        static std::unique_ptr<ChannelStateWriteRequest> terminate(
            JobVertexID jobVertexID, int subtaskIndex, long checkpointId, const std::exception_ptr &cause);

        static std::unique_ptr<ChannelStateWriteRequest> registerSubtask(
            JobVertexID jobVertexID, int subtaskIndex);

        static std::unique_ptr<ChannelStateWriteRequest> releaseSubtask(
            JobVertexID jobVertexID, int subtaskIndex);

    private:
        JobVertexID jobVertexID_;
        int subtaskIndex_;
        long checkpointId_;
        std::string name_;
    };

    class CheckpointStartRequest : public ChannelStateWriteRequest {
    public:
        CheckpointStartRequest(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            ChannelStateWriter::ChannelStateWriteResult &targetResult,
            CheckpointStorageLocationReference locationReference);

        ChannelStateWriter::ChannelStateWriteResult &getTargetResult();
        CheckpointStorageLocationReference &getLocationReference();

        void cancel(const std::exception_ptr &cause) override;
        void execute(ChannelStateCheckpointWriter &writer) override;

    private:
        ChannelStateWriter::ChannelStateWriteResult &targetResult_;
        CheckpointStorageLocationReference locationReference_;
    };

    class CheckpointInProgressRequest : public ChannelStateWriteRequest {
    public:
        using Action = std::function<void(ChannelStateCheckpointWriter &)>;
        using DiscardAction = std::function<void(const std::exception_ptr &)>;

        CheckpointInProgressRequest(
            const std::string &name,
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            Action action,
            DiscardAction discardAction,
            std::shared_ptr<CompletableFutureV2<void>> readyFuture = nullptr);

        CheckpointInProgressRequest(
            const std::string &name,
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            Action action);

        CheckpointInProgressRequest(
            const std::string &name,
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            Action action,
            DiscardAction discardAction,
            std::shared_ptr<CompletableFutureV2<std::vector<ObjectBuffer>>> dataFuture);

        std::shared_ptr<CompletableFutureV2<void>> getReadyFuture() override;
        void cancel(const std::exception_ptr &cause) override;
        void execute(ChannelStateCheckpointWriter &writer) override;

    private:
        Action action_;
        DiscardAction discardAction_;
        std::shared_ptr<CompletableFutureV2<void>> readyFuture_;
        std::shared_ptr<CompletableFutureV2<std::vector<ObjectBuffer>>> dataFuture_;
        std::atomic<CheckpointInProgressRequestState> state_{CheckpointInProgressRequestState::NEW};
    };

    class CheckpointAbortRequest : public ChannelStateWriteRequest {
    public:
        CheckpointAbortRequest(
            JobVertexID jobVertexID,
            int subtaskIndex,
            long checkpointId,
            const std::exception_ptr &cause);

        const std::exception_ptr &getCause() const;
        void cancel(const std::exception_ptr &) override;
        void execute(ChannelStateCheckpointWriter &writer) override;

    private:
        std::exception_ptr cause_;
    };

    class SubtaskRegisterRequest : public ChannelStateWriteRequest {
    public:
        SubtaskRegisterRequest(JobVertexID jobVertexID, int subtaskIndex);
        void cancel(const std::exception_ptr &) override;
        void execute(ChannelStateCheckpointWriter &writer) override;
    };

    class SubtaskReleaseRequest : public ChannelStateWriteRequest {
    public:
        SubtaskReleaseRequest(JobVertexID jobVertexID, int subtaskIndex);
        void cancel(const std::exception_ptr &) override;
        void execute(ChannelStateCheckpointWriter &writer) override;
    };

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_H