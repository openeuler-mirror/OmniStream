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
#pragma once
#include "LocalInputChannel.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include <optional>
#include <memory>
#include <queue>

#include "RemoteDataFetcherBridge.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/watermark/Watermark.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "runtime/buffer/ObjectBufferRecycler.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"


namespace omnistream {
    class RemoteInputChannel : public LocalInputChannel {
    public:
        RemoteInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                           ResultPartitionIDPOD partitionId,
                           std::shared_ptr<ResultPartitionManager> partitionManager,
                           int initialBackoff, int maxBackoff, int networkBuffersPerChannel,
                           std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn,
                           std::shared_ptr<ChannelStateWriter> stateWriter
        );
        void requestSubpartition(int subpartitionIndex) override;
        void notifyRemoteDataAvailableForVectorBatch(long bufferAddress, int bufferLength, int sequenceNumber);
        std::optional<BufferAndAvailability> getNextBuffer() override;
        std::shared_ptr<ObjectSegment> DoDataDeserializationResult(uint8_t*& buffer, int bufferLength);
        void notifyRemoteDataAvailableForNetworkBuffer(long bufferAddress, int bufferLength, int readIndex,
                                                       int sequenceNumber,
                                                       std::shared_ptr<OriginalNetworkBufferRecycler>
                                                       originalNetworkBufferRecycler, bool isBuffer, int bufferType);

        void SetRemoteDataFetcherBridge(std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge);
        void resumeConsumption() override;
        void CheckpointStarted(const CheckpointBarrier& barrier) override;
        void CheckpointStopped(long checkpointId) override;
        std::vector<Buffer*> GetInflightBuffersUnsafe(long checkpointId);

        void ResetLastBarrier()
        {
            lastBarrierId_ = 1;
        }
    private:
        std::queue<Buffer*> dataQueue;
        int expectSequenceNumber = 0;
        int initialCredit;
        std::recursive_mutex queueMutex;
        std::shared_ptr<RemoteDataFetcherBridge> remoteDataFetcherBridge;
        long lastBarrierId_ = -1;
    };
};
