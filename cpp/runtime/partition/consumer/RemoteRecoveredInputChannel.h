//
// Created by h30059066 on 2026/2/9.
//

#ifndef OMNISTREAM_REMOTERECOVEREDINPUTCHANNEL_H
#define OMNISTREAM_REMOTERECOVEREDINPUTCHANNEL_H

#include "RecoveredInputChannel.h"
#include "runtime/partition/ResultPartitionManager.h"
#include "RemoteInputChannel.h"
#include "iostream"
#include "checkpoint/channel/ChannelStateWriterImpl.h"

class RemoteRecoveredInputChannel : public RecoveredInputChannel {
public:
    RemoteRecoveredInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex,
                                ResultPartitionIDPOD partitionId,
                                std::shared_ptr<omnistream::ResultPartitionManager> manager,
                                int initialBackoff, int maxBackoff, int networkBuffersPerChannel,
                                std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn
    ) : RecoveredInputChannel(inputGate, channelIndex, partitionId, consumedSubpartitionIndex, initialBackoff,
                              maxBackoff, numBytesIn, numBuffersIn, networkBuffersPerChannel),partitionManager(manager) {}

    std::shared_ptr<omnistream::InputChannel> toInputChannelInternal()
    {
        auto stateWriter = GetChannelStateWriter();
        if (stateWriter == nullptr) {
            LOG("ERROR: RemoteRecoveredInputChannel.h stateWriter is null.")
            throw std::runtime_error("ERROR: RemoteRecoveredInputChannel.h stateWriter is null.");
        }

        std::cout <<"convert to RemoteInputChannel"<<std::endl;
        return std::make_shared<omnistream::RemoteInputChannel>(inputGate, getChannelIndex(), partitionId,
                                                                partitionManager, initialBackoff, maxBackoff,
                                                                getNetworkBuffersPerChannel(), numBytesIn,
                                                                numBuffersIn,stateWriter);
    }
private:
    std::shared_ptr<omnistream::ResultPartitionManager> partitionManager;
};

#endif //OMNISTREAM_REMOTERECOVEREDINPUTCHANNEL_H