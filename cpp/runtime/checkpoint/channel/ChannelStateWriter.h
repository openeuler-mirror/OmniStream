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

#ifndef OMNISTREAM_CHANNELSTATEWRITER_H
#define OMNISTREAM_CHANNELSTATEWRITER_H
#include <future>
#include <vector>
#include <memory>
#include <exception>
#include <functional>
#include <stdexcept>
#include <iostream>
#include "../CheckpointOptions.h"
#include "runtime/state/ResultSubpartitionStateHandle.h"
#include "runtime/state/InputChannelStateHandle.h"
#include "runtime/partition/ResultSubpartitionInfoPOD.h"
#include "runtime/partition/consumer/InputChannelInfo.h"
#include "core/utils/lang/AutoCloseable.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/buffer/ObjectBuffer.h"
#include "core/utils/threads/CompletableFuture.h"
#include "runtime/state/CheckpointStorageWorkerView.h"

/** Writes channel state during checkpoint/savepoint. */
// @Internal
class ChannelStateWriter {
public:
    /** Channel state write result. */
    class ChannelStateWriteResult {
    public:
        using InputChannelStateHandleVecPtr = std::shared_ptr<std::vector<InputChannelStateHandle>>;
        using ResultSubpartitionStateVecPtr = std::shared_ptr<std::vector<ResultSubpartitionStateHandle>>;
        using InputChannelStateFuture = std::shared_ptr<CompletableFutureV2<InputChannelStateHandleVecPtr>>;
        using ResultSubpartitionStateFuture = std::shared_ptr<CompletableFutureV2<ResultSubpartitionStateVecPtr>>;

        ChannelStateWriteResult() noexcept
            : inputChannelStateHandles(),
              resultSubpartitionStateHandles()
        {}

        ChannelStateWriteResult(
            InputChannelStateFuture inputChannelStateHandles,
            ResultSubpartitionStateFuture resultSubpartitionStateHandles) noexcept
            : inputChannelStateHandles(std::move(inputChannelStateHandles)),
              resultSubpartitionStateHandles(std::move(resultSubpartitionStateHandles))
        {}

        InputChannelStateFuture GetInputChannelStateHandles()
        {
            return inputChannelStateHandles;
        }

        ResultSubpartitionStateFuture GetResultSubpartitionStateHandles()
        {
            return resultSubpartitionStateHandles;
        }

        static ChannelStateWriteResult CreateEmpty() noexcept
        {
            auto inputFuture = std::make_shared<CompletableFutureV2<InputChannelStateHandleVecPtr>>();
            auto resultFuture = std::make_shared<CompletableFutureV2<ResultSubpartitionStateVecPtr>>();
            
            return ChannelStateWriteResult(inputFuture, resultFuture);
        }

        void Fail(const std::exception_ptr& cause)
        {
            inputChannelStateHandles->Cancel(cause);
            resultSubpartitionStateHandles->Cancel(cause);
        }

        bool IsDone() const
        {
            return inputChannelStateHandles->IsDone() && resultSubpartitionStateHandles->IsDone();
        }
    private:
        InputChannelStateFuture inputChannelStateHandles;
        ResultSubpartitionStateFuture resultSubpartitionStateHandles;
    };

    /**
     * Sequence number for the buffers that were saved during the previous execution attempt; then
     * restored; and now are to be saved again (as opposed to the buffers received from the upstream
     * or from the operator).
     */
    static const int sequenceNumberRestored = -1;

    /**
     * Signifies that buffer sequence number is unknown (e.g. if passing sequence numbers is not
     * implemented).
     */
    static const int sequenceNumberUnknown = -2;

    static ChannelStateWriteResult empty;
    /** Initiate write of channel state for the given checkpoint id. */
    virtual void Start(long checkpointId, const CheckpointOptions& checkpointOptions) = 0;

    virtual ~ChannelStateWriter() = default;

    /**
     * Add in-flight buffers from the {@link
     * org.apache.flink.runtime.io.network.partition.consumer.InputChannel InputChannel}. Must be
     * called after {@link #start} (long)} and before {@link #finishInput(long)}. Buffers are
     * recycled after they are written or exception occurs.
     *
     * @param startSeqNum sequence number of the 1st passed buffer. It is intended to use for
     *     incremental snapshots. If no data is passed it is ignored.
     * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberRestored
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberUnknown
     */
    virtual void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info, int startSeqNum,
        std::vector<omnistream::ObjectBuffer*> data) = 0;

    /**
     * Add in-flight buffers from the {@link
     * org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}. Must be
     * called after {@link #start} and before {@link #finishOutput(long)}. Buffers are recycled
     * after they are written or exception occurs.
     *
     * @param startSeqNum sequence number of the 1st passed buffer. It is intended to use for
     *     incremental snapshots. If no data is passed it is ignored.
     * @param data zero or more <b>data</b> buffers ordered by their sequence numbers
     * @throws IllegalArgumentException if one or more passed buffers {@link Buffer#isBuffer() isn't
     *     a buffer}
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberRestored
     * @see org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter#sequenceNumberUnknown
     */
    virtual void AddOutputData(long checkpointId, const omnistream::ResultSubpartitionInfoPOD& info, int startSeqNum,
        std::vector<omnistream::ObjectBuffer*>& data) = 0;

    /**
     * Add in-flight bufferFuture from the {@link
     * org.apache.flink.runtime.io.network.partition.ResultSubpartition ResultSubpartition}. Must be
     * called after {@link #start} and before {@link #finishOutput(long)}. Buffers are recycled
     * after they are written or exception occurs.
     *
     * <p>The method will be called when the unaligned checkpoint is enabled and received an aligned
     * barrier.
     */
    virtual void AddOutputDataFuture(long checkpointId, const omnistream::ResultSubpartitionInfoPOD& info,
        int startSeqNum, std::shared_ptr<CompletableFutureV2<std::vector<omnistream::ObjectBuffer*>>> data) = 0;

    /**
     * Finalize write of channel state data for the given checkpoint id. Must be called after {@link
     * #start(long, CheckpointOptions)} and all of the input data of the given checkpoint added.
     * When both {@link #finishInput} and {@link #finishOutput} were called the results can be
     * (eventually) obtained using {@link #getAndRemoveWriteResult}
     */
    virtual void FinishInput(long checkpointId) = 0;

    /**
     * Finalize write of channel state data for the given checkpoint id. Must be called after {@link
     * #start(long, CheckpointOptions)} and all of the output data of the given checkpoint added.
     * When both {@link #finishInput} and {@link #finishOutput} were called the results can be
     * (eventually) obtained using {@link #getAndRemoveWriteResult}
     */
    virtual void FinishOutput(long checkpointId) = 0;

    /**
     * Aborts the checkpoint and fails pending result for this checkpoint.
     *
     * @param cleanup true if {@link #getAndRemoveWriteResult(long)} is not supposed to be called
     *     afterwards.
     */
    virtual void Abort(long checkpointId, const std::exception_ptr& cause, bool cleanup) = 0;

    /**
     * Must be called after {@link #start(long, CheckpointOptions)} once.
     *
     * @throws IllegalArgumentException if the passed checkpointId is not known.
     */
    virtual ChannelStateWriteResult GetAndRemoveWriteResult(long checkpointId) = 0;
};

/** No-op implementation of {@link ChannelStateWriter}. */
class NoOpChannelStateWriter : public ChannelStateWriter {
public:
   static NoOpChannelStateWriter *noOp;

    void Start(long checkpointId, const CheckpointOptions& checkpointOptions) override
    {}

    void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info, int startSeqNum,
        std::vector<omnistream::ObjectBuffer*> data) override
    {}

    void AddOutputData(long checkpointId, const omnistream::ResultSubpartitionInfoPOD& info, int startSeqNum,
        std::vector<omnistream::ObjectBuffer*>& data) override
    {}

    void AddOutputDataFuture(long checkpointId, const omnistream::ResultSubpartitionInfoPOD& info, int startSeqNum,
        std::shared_ptr<CompletableFutureV2<std::vector<omnistream::ObjectBuffer*>>> data) override
    {}

    void FinishInput(long checkpointId) override
    {}

    void FinishOutput(long checkpointId) override
    {}

    void Abort(long checkpointId, const std::exception_ptr& cause, bool cleanup) override
    {}

    ChannelStateWriteResult GetAndRemoveWriteResult(long checkpointId) override
    {
        return ChannelStateWriteResult::CreateEmpty();
    }
};

#endif // OMNISTREAM_CHANNELSTATEWRITER_H
