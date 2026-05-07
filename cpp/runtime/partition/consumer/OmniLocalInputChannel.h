/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef OMNILOCALINPUTCHANNEL_H
#define OMNILOCALINPUTCHANNEL_H
#include "LocalInputChannel.h"
#include "runtime/buffer/OriginalNetworkBufferRecycler.h"
#include "runtime/state/bridge/OmniLocalInputChannelBridge.h"

namespace omnistream {
    class OmniLocalInputChannel : public LocalInputChannel {
    public:
        OmniLocalInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                              ResultPartitionIDPOD partitionId,
                              std::shared_ptr<ResultPartitionManager> partitionManager,
                              int initialBackoff, int maxBackoff, int networkBuffersPerChannel,
                              std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn,
                              std::shared_ptr<ChannelStateWriter> stateWriter
        );

        void requestSubpartition(int subpartitionIndex) override;

        void notifyOriginalDataAvailable(long bufferAddress, int bufferLength, int readIndex, int sequenceNumber,
                                         int memorySegmentOffset, int bufferType);

        std::optional<BufferAndAvailability> getNextBuffer() override;

        std::shared_ptr<ObjectSegment> DoDataDeserializationResult(uint8_t *&buffer, int bufferLength);

        long GetRecycleBufferAddress();

        void releaseAllResources() override;

        void resumeConsumption() override;

        void SetOmniLocalInputChannelBridge(std::shared_ptr<OmniLocalInputChannelBridge> omniLocalInputChannelBridge);

        void CheckpointStarted(const CheckpointBarrier& barrier, std::shared_ptr<ChannelStateWriter> channelStateWriter);
        
        void CheckpointStopped(long checkpointId);

        std::vector<Buffer*> GetInflightBuffersUnsafe(long checkpointId);

        void SetPersistenceFlag(bool flag)
        {
            isNeedPersistence_ = flag;
        }
        bool IsNeedPersistence()
        {
            if (startSize_ != 0) {
                return outsize < startSize_;
            }
            return true;
        }
        void AddInputData(long checkpointId, const omnistream::InputChannelInfo& info);
    private:
        size_t insize = 0;
        size_t outsize = 0;
        size_t startSize_ = 0;
        bool isNeedPersistence_ = false;
        int expectSequenceNumber = 0;
        int initialCredit;
        std::recursive_mutex queueMutex;
        std::shared_ptr<OriginalNetworkBufferRecycler> originalNetworkBufferRecycler_;
        std::queue<std::shared_ptr<BufferAndAvailability> > dataQueue;
        std::shared_ptr<OmniLocalInputChannelBridge> omniLocalInputChannelBridge;
        std::vector<Buffer*> inflightBuffers_;
    };
}
#endif // OMNILOCALINPUTCHANNEL_H
