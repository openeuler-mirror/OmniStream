/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_INDEXEDINPUTGATE_H
#define OMNISTREAM_INDEXEDINPUTGATE_H

#include <memory>
#include <vector>
#include <string>
#include "InputGate.h"

#include "InputChannelInfo.h"

#include "InputChannel.h"
#include <iostream>

namespace omnistream {

    class IndexedInputGate : public InputGate {
    public:
        IndexedInputGate() = default;
        ~IndexedInputGate() override = default;

        virtual int getGateIndex()  = 0;
        virtual std::vector<InputChannelInfo> getUnfinishedChannels()  = 0;

        int getInputGateIndex()   {   // checkinto related , do it ilater
            return getGateIndex();
        }

        void blockConsumption(const InputChannelInfo& channelInfo)  {   // checkinto related , do it ilater
        }

        void convertToPriorityEvent(int channelIndex, int sequenceNumber)  {   // checkinto related , do it ilater
            getChannel(channelIndex)->convertToPriorityEvent(sequenceNumber);
        }

        virtual int getBuffersInUseCount()  = 0;
        virtual void announceBufferSize(int bufferSize) = 0;

        std::string toString()  override {
            return "IndexedInputGate [gateIndex=" + std::to_string(getGateIndex()) + "]";
        }
    };

} // namespace omnistream

#endif // OMNISTREAM_INDEXEDINPUTGATE_H