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
        
        void SetForwardResumeToJava(bool forwardResumeToJava) {
            forwardResumeToJava_ = forwardResumeToJava;
        }

    private:
        int expectSequenceNumber = 0;
        int initialCredit;
        std::recursive_mutex queueMutex;
        std::shared_ptr<OriginalNetworkBufferRecycler> originalNetworkBufferRecycler_;
        std::queue<std::shared_ptr<BufferAndAvailability> > dataQueue;
        std::shared_ptr<OmniLocalInputChannelBridge> omniLocalInputChannelBridge;
        bool forwardResumeToJava_ = true;
    };
}
#endif // OMNILOCALINPUTCHANNEL_H
