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
#ifndef OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_IMPL_H
#define OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_IMPL_H

#include <map>
#include <mutex>
#include <set>
#include <exception>
#include <stdexcept>
#include "ChannelStateCheckpointWriter.h"
#include "ChannelStateWriteRequest.h"
#include "ChannelStateWriteRequestDispatcher.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/CheckpointStorageWorkerView.h"
#include "runtime/io/checkpointing/CheckpointException.h"

namespace omnistream {

    class ChannelStateWriteRequestDispatcherImpl : public ChannelStateWriteRequestDispatcher {
    public:
        ChannelStateWriteRequestDispatcherImpl(
            CheckpointStorage *checkpointStorage,
            const JobIDPOD &jobID,
            ChannelStateSerializer *serializer);

        void dispatch(ChannelStateWriteRequest &request) override;
        void fail(const std::exception_ptr &cause) override;

    private:
        CheckpointStorage *checkpointStorage;
        JobIDPOD jobID;
        ChannelStateSerializer *serializer;
        std::set<SubtaskID> registeredSubtasks;
        std::mutex mutex;
        long ongoingCheckpointId;
        long maxAbortedCheckpointId;
        SubtaskID abortedSubtaskID;
        std::exception_ptr abortedCause;
        std::unique_ptr<ChannelStateCheckpointWriter> writer;
        CheckpointStorageWorkerView *streamFactoryResolver;

        void dispatchInternal(ChannelStateWriteRequest &request);
        bool isAbortedCheckpoint(long checkpointId);
        void handleAbortedRequest(ChannelStateWriteRequest &request);
        void handleCheckpointStartRequest(CheckpointStartRequest &request);
        void handleCheckpointInProgressRequest(CheckpointInProgressRequest &request);
        void handleCheckpointAbortRequest(CheckpointAbortRequest &request);
        void failAndClearWriter(const std::exception_ptr &e);
        void failAndClearWriter(const JobVertexID &jvid, int idx, const std::exception_ptr &e);
        std::unique_ptr<ChannelStateCheckpointWriter> buildWriter(CheckpointStartRequest &request);
        CheckpointStorageWorkerView *getStreamFactoryResolver();
    };

} // namespace omnistream

#endif // OMNISTREAM_CHANNEL_STATE_WRITE_REQUEST_DISPATCHER_IMPL_H