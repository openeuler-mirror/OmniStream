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
#include "ChannelStateWriteRequestDispatcherImpl.h"

namespace omnistream {

    ChannelStateWriteRequestDispatcherImpl::ChannelStateWriteRequestDispatcherImpl(
        CheckpointStorage *checkpointStorage,
        const JobIDPOD &jobID,
        ChannelStateSerializer *serializer)
        : checkpointStorage(checkpointStorage),
          jobID(jobID),
          serializer(serializer),
          ongoingCheckpointId(-1),
          maxAbortedCheckpointId(-1),
          abortedSubtaskID(JobVertexID(-1, -1), -1),
          streamFactoryResolver(nullptr) {}

    void ChannelStateWriteRequestDispatcherImpl::dispatch(ChannelStateWriteRequest &request)
    {
        try {
            dispatchInternal(request);
        } catch (...) {
            request.cancel(std::current_exception());
        }
    }

    void ChannelStateWriteRequestDispatcherImpl::fail(const std::exception_ptr &cause)
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (writer) {
            writer->Fail(cause);
            writer.reset();
        }
    }

    void ChannelStateWriteRequestDispatcherImpl::dispatchInternal(ChannelStateWriteRequest &request)
    {
        std::lock_guard<std::mutex> lock(mutex);

        if (auto *req = dynamic_cast<SubtaskRegisterRequest *>(&request)) {
            SubtaskID subtaskID = SubtaskID::Of(req->getJobVertexID(), req->getSubtaskIndex());
            registeredSubtasks.insert(subtaskID);
            return;
        }

        if (auto *req = dynamic_cast<SubtaskReleaseRequest *>(&request)) {
            SubtaskID subtaskID = SubtaskID::Of(req->getJobVertexID(), req->getSubtaskIndex());
            registeredSubtasks.erase(subtaskID);
            if (writer) {
                writer->ReleaseSubtask(subtaskID);
            }
            return;
        }

        if (isAbortedCheckpoint(request.getCheckpointId())) {
            handleAbortedRequest(request);
        } else if (auto *startReq = dynamic_cast<CheckpointStartRequest *>(&request)) {
            handleCheckpointStartRequest(*startReq);
        } else if (auto *inProgressReq = dynamic_cast<CheckpointInProgressRequest *>(&request)) {
            handleCheckpointInProgressRequest(*inProgressReq);
        } else if (auto *abortReq = dynamic_cast<CheckpointAbortRequest *>(&request)) {
            handleCheckpointAbortRequest(*abortReq);
        } else {
            throw std::invalid_argument("Unknown request type");
        }
    }

    bool ChannelStateWriteRequestDispatcherImpl::isAbortedCheckpoint(long checkpointId)
    {
        return checkpointId < ongoingCheckpointId || checkpointId <= maxAbortedCheckpointId;
    }

    void ChannelStateWriteRequestDispatcherImpl::handleAbortedRequest(ChannelStateWriteRequest &request)
    {
        if (request.getCheckpointId() != maxAbortedCheckpointId) {
            request.cancel(std::make_exception_ptr(CheckpointException(CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED)));
            return;
        }

        if (SubtaskID::Of(request.getJobVertexID(), request.getSubtaskIndex()) == abortedSubtaskID) {
            request.cancel(std::make_exception_ptr(abortedCause));
        } else {
            request.cancel(std::make_exception_ptr(
                CheckpointException(
                    CheckpointFailureReason::CHANNEL_STATE_SHARED_STREAM_EXCEPTION,
                    std::runtime_error("Aborted by another subtask"))));
        }
    }

    void ChannelStateWriteRequestDispatcherImpl::handleCheckpointStartRequest(CheckpointStartRequest &request)
    {
        if (request.getCheckpointId() < ongoingCheckpointId) {
            throw std::logic_error("Checkpoint ID must be incremental. Ongoing: " + std::to_string(ongoingCheckpointId) + ", Requested: " + std::to_string(request.getCheckpointId()));
        }

        if (request.getCheckpointId() > ongoingCheckpointId) {
            failAndClearWriter(std::make_exception_ptr(CheckpointException(CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED)));
        }

        if (!writer) {
            writer = buildWriter(request);
            ongoingCheckpointId = request.getCheckpointId();
        }

        writer->RegisterSubtaskResult(
            SubtaskID::Of(request.getJobVertexID(), request.getSubtaskIndex()),
            request.getTargetResult());
    }

    void ChannelStateWriteRequestDispatcherImpl::handleCheckpointInProgressRequest(CheckpointInProgressRequest &request)
    {
        if (!writer || ongoingCheckpointId != request.getCheckpointId()) {
            throw std::logic_error("Writer not found for request");
        }
        request.execute(*writer);
    }

    void ChannelStateWriteRequestDispatcherImpl::handleCheckpointAbortRequest(CheckpointAbortRequest &request)
    {
        if (request.getCheckpointId() > maxAbortedCheckpointId) {
            maxAbortedCheckpointId = request.getCheckpointId();
            abortedCause = request.getCause();
            abortedSubtaskID = SubtaskID::Of(
                request.getJobVertexID(), request.getSubtaskIndex());
        }

        if (request.getCheckpointId() == ongoingCheckpointId) {
            try {
                std::rethrow_exception(request.getCause());
            } catch (const std::exception &e) {
                failAndClearWriter(
                    request.getJobVertexID(),
                    request.getSubtaskIndex(),
                    request.getCause());
            }
        } else if (request.getCheckpointId() > ongoingCheckpointId) {
            failAndClearWriter(std::make_exception_ptr(CheckpointException(CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED)));
        }
    }

    void ChannelStateWriteRequestDispatcherImpl::failAndClearWriter(const std::exception_ptr &e)
    {
        if (writer) {
            writer->Fail(e);
            writer.reset();
        }
    }

    void ChannelStateWriteRequestDispatcherImpl::failAndClearWriter(const JobVertexID &jvid, int idx, const std::exception_ptr &e)
    {
        if (writer) {
            writer->Fail(jvid, idx, e);
            writer.reset();
        }
    }

    std::unique_ptr<ChannelStateCheckpointWriter> ChannelStateWriteRequestDispatcherImpl::buildWriter(CheckpointStartRequest &request)
    {
        return std::make_unique<ChannelStateCheckpointWriter>(
            registeredSubtasks,
            request.getCheckpointId(),
            getStreamFactoryResolver()->resolveCheckpointStorageLocation(request.getCheckpointId(), request.getLocationReference()),
            serializer,
            [this]() { writer.reset(); });
    }

    CheckpointStorageWorkerView *ChannelStateWriteRequestDispatcherImpl::getStreamFactoryResolver()
    {
        if (!streamFactoryResolver) {
            streamFactoryResolver = checkpointStorage->createCheckpointStorage(jobID);
        }
        return streamFactoryResolver;
    }

} // namespace omnistream
