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
#include "ChannelStateCheckpointWriter.h"
#include "ChannelStateWriter.h"
#include "state/filesystem/FileStateHandle.h"
namespace omnistream {

    ChannelStateCheckpointWriter::ChannelStateCheckpointWriter(
        const std::set<SubtaskID> &subtasks,
        int64_t checkpointId,
        CheckpointStreamFactory *streamFactory,
        std::shared_ptr<ChannelStateSerializer> serializer,
        std::function<void()> onComplete)
        : checkpointId(checkpointId), serializer(serializer), onComplete(onComplete)
    {
        if (subtasks.empty()) {
            throw std::invalid_argument("subtasks cannot be empty");
        }
        subtasksToRegister = subtasks;
        checkpointStream = streamFactory->createCheckpointStateOutputStream(CheckpointedStateScope::EXCLUSIVE);
        dataStream = new std::ostringstream();
        serializer->WriteHeader(*dataStream);
    }

    ChannelStateCheckpointWriter::~ChannelStateCheckpointWriter()
    {
        delete dataStream;
        delete checkpointStream;
        for (auto &kv : pendingResults) {
            delete kv.second;
        }
    }

    void ChannelStateCheckpointWriter::RegisterSubtaskResult(const SubtaskID &id,
                                                             std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> result)
    {
        if (IsDone()) {
            throw std::logic_error("Write is already done");
        }
        if (pendingResults.count(id)) {
            throw std::logic_error("Subtask already registered");
        }
        subtasksToRegister.erase(id);
        pendingResults[id] = new ChannelStatePendingResult(
            id.GetSubtaskIndex(), checkpointId, result, serializer);
    }

    void ChannelStateCheckpointWriter::ReleaseSubtask(const SubtaskID &id)
    {
        subtasksToRegister.erase(id);
        TryFinishResult();
    }

    void ChannelStateCheckpointWriter::WriteInput(const JobVertexID &jvid,
                                                  int subtaskIndex,
                                                  const InputChannelInfo &info,
                                                  Buffer* buffer)
    {
        if (IsDone()) {
            buffer->RecycleBuffer();
            return;
        }

        ChannelStatePendingResult *pending = GetChannelStatePendingResult(jvid, subtaskIndex);
        Write(pending->GetInputChannelOffsets(),
              info,
              buffer,
              !pending->IsAllInputsReceived(),
              "ChannelState#WriteInput");

        buffer->RecycleBuffer();
    }

    void ChannelStateCheckpointWriter::WriteOutput(const JobVertexID &jvid,
                                                   int subtaskIndex,
                                                   const ResultSubpartitionInfoPOD &info,
                                                   Buffer* buffer)
    {
        if (IsDone()) {
            buffer->RecycleBuffer();
            return;
        }

        ChannelStatePendingResult *pending = GetChannelStatePendingResult(jvid, subtaskIndex);
        Write(pending->GetResultSubpartitionOffsets(), info, buffer,
              !pending->IsAllOutputsReceived(), "ChannelState#WriteOutput");
        buffer->RecycleBuffer();
    }

    void ChannelStateCheckpointWriter::CompleteInput(const JobVertexID &jvid, int subtaskIndex)
    {
        if (IsDone()) {
            return;
        }

        GetChannelStatePendingResult(jvid, subtaskIndex)->CompleteInput();
        TryFinishResult();
    }

    void ChannelStateCheckpointWriter::CompleteOutput(const JobVertexID &jvid, int subtaskIndex)
    {
        if (IsDone()) {
            return;
        }

        GetChannelStatePendingResult(jvid, subtaskIndex)->CompleteOutput();
        TryFinishResult();
    }

    void ChannelStateCheckpointWriter::Fail(const JobVertexID &jvid, int subtaskIndex, const std::exception_ptr &e)
    {
        if (IsDone()) {
            return;
        }
        throwable = e;
        auto id = SubtaskID::Of(jvid, subtaskIndex);
        if (pendingResults.count(id)) {
            pendingResults[id]->Fail(e);
        }
        failResultAndCloseStream(e);
    }

    void ChannelStateCheckpointWriter::Fail(const std::exception_ptr &e)
    {
        if (IsDone()) {
            return;
        }
        throwable = e;
        failResultAndCloseStream(e);
    }
    void ChannelStateCheckpointWriter::Reset()
    {

    }
    void ChannelStateCheckpointWriter::Start(const JobVertexID &jobVertexID,
                                             int subtaskIndex,
                                             std::shared_ptr<ChannelStateWriter::ChannelStateWriteResult> targetResult,
                                             std::shared_ptr<CheckpointStorageLocationReference> locationReference)
    {
        SubtaskID id = SubtaskID::Of(jobVertexID, subtaskIndex);
        RegisterSubtaskResult(id, targetResult);
    }

    void ChannelStateCheckpointWriter::Abort(const JobVertexID &jobVertexID,
                                             int subtaskIndex,
                                             const std::exception_ptr &cause)
    {
        Fail(jobVertexID, subtaskIndex, cause);
    }

    void ChannelStateCheckpointWriter::RegisterSubtask(const JobVertexID &jobVertexID, int subtaskIndex) {}

    bool ChannelStateCheckpointWriter::IsDone() const
    {
        if (throwable) {
            return true;
        }
        for (auto &kv : pendingResults)
            if (kv.second->IsDone()) {
                return true;
            }
        return false;
    }

    void ChannelStateCheckpointWriter::TryFinishResult()
    {
        if (!subtasksToRegister.empty()) {
            return;
        }

        for (auto &kv : pendingResults) {
            if (!kv.second->IsAllInputsReceived() ||
                !kv.second->IsAllOutputsReceived()) {
                return;
            }
        }

        if (IsDone()) {
            onComplete();
        } else {
            FinishWriteAndResult();
            onComplete();
        }
    }

    void ChannelStateCheckpointWriter::FinishWriteAndResult()
    {
        StreamStateHandle *handle = nullptr;
        if (dataStream->str().size() == static_cast<size_t>(serializer->GetHeaderLength())) {
            checkpointStream->Close();
        } else {
            checkpointStream->Flush();
            handle = checkpointStream->CloseAndGetHandle();
        }
    }

    void ChannelStateCheckpointWriter::failResultAndCloseStream(const std::exception_ptr &e)
    {
        // for (auto &kv : pendingResults)
            // kv.second->Fail(e);
        try {
            checkpointStream->Close();
        } catch (const std::exception &ex) {
            std::cerr << "Failed to close checkpointStream: " << ex.what() << std::endl;
        }
    }

    ChannelStatePendingResult *ChannelStateCheckpointWriter::GetChannelStatePendingResult(const JobVertexID &jvid, int subtaskIndex)
    {
        auto id = SubtaskID::Of(jvid, subtaskIndex);
        auto it = pendingResults.find(id);
        if (it == pendingResults.end()) {
            LOG_DEBUG("ChannelStateCheckpointWriter::GetChannelStatePendingResult")
            throw std::runtime_error("Subtask not registered: " + id.ToString());
        }
        return it->second;
    }

} // namespace omnistream
