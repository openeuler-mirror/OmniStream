/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once
#include "LocalInputChannel.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include <optional>
#include <memory>
#include <queue>
#include <arm_sve.h>
#include "core/streamrecord/StreamRecord.h"
#include "include/functions/Watermark.h"
#include "include/functions/StreamElement.h"
#include "runtime/buffer/ObjectBufferRecycler.h"

namespace omnistream {
    class RemoteInputChannel : public LocalInputChannel {
    public:
        RemoteInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                           ResultPartitionIDPOD partitionId,
                           std::shared_ptr<ResultPartitionManager> partitionManager,
                           int initialBackoff, int maxBackoff, int networkBuffersPerChannel,
                           std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn
        );
        void requestSubpartition(int subpartitionIndex) override;
        void notifyRemoteDataAvailable(long bufferAddress, int bufferLength, int sequenceNumber);
        std::optional<BufferAndAvailability> getNextBuffer() override;
        std::shared_ptr<ObjectSegment> DoDataDeserializationResult(uint8_t*& buffer, int bufferLength);
        void getResData(void* dst, void* src, size_t cur, int res);

    private:
        std::queue<std::shared_ptr<VectorBatchBuffer>> dataQueue;
        int expectSequenceNumber = 0;
        int initialCredit;
        std::recursive_mutex queueMutex;
    };
};
