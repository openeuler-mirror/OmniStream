//
// Created by h30059066 on 2026/2/9.
//

#ifndef OMNISTREAM_LOCALRECOVEREDINPUTCHANNEL_H
#define OMNISTREAM_LOCALRECOVEREDINPUTCHANNEL_H
#include "RecoveredInputChannel.h"
#include <partition/ResultPartitionManager.h>
#include "LocalInputChannel.h"
#include "OmniLocalInputChannel.h"
#include "iostream"
#include "checkpoint/channel/ChannelStateWriterImpl.h"

class LocalRecoveredInputChannel : public RecoveredInputChannel {
public:
    LocalRecoveredInputChannel(std::shared_ptr<omnistream::SingleInputGate> inputGate, int channelIndex,
                               omnistream::ResultPartitionIDPOD partitionId,
                               int consumedSubpartitionIndex, std::shared_ptr<omnistream::ResultPartitionManager> manager, int initialBackoff, int maxBackoff,
                               std::shared_ptr<omnistream::Counter> numBytesIn,
                               std::shared_ptr<omnistream::Counter> numBuffersIn, int networkBuffersPerChannel)
            : RecoveredInputChannel(inputGate, channelIndex, partitionId, consumedSubpartitionIndex, initialBackoff,
                                    maxBackoff, numBytesIn, numBuffersIn, networkBuffersPerChannel),partitionManager(manager){

    }

    std::shared_ptr<omnistream::InputChannel> toInputChannelInternal()
    {
        auto stateWriter = GetChannelStateWriter();
        if (stateWriter == nullptr) {
            LOG("ERROR: LocalRecoveredInputChannel stateWriter is null.")
            throw std::runtime_error("ERROR: LocalRecoveredInputChannel stateWriter is null.");
        }

        return std::make_shared<omnistream::LocalInputChannel>(inputGate, getChannelIndex(), partitionId,
                                                               partitionManager, initialBackoff, maxBackoff, numBytesIn,
                                                               numBuffersIn, stateWriter);
    }

private:
    std::shared_ptr<omnistream::ResultPartitionManager> partitionManager;
};

#endif //OMNISTREAM_LOCALRECOVEREDINPUTCHANNEL_H