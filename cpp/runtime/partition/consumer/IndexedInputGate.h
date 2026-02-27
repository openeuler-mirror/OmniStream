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

#ifndef OMNISTREAM_INDEXEDINPUTGATE_H
#define OMNISTREAM_INDEXEDINPUTGATE_H

#include <memory>
#include <vector>
#include <string>
#include <iostream>

#include "InputChannelInfo.h"
#include "runtime/io/network/partition/consumer/CheckpointableInput.h"
#include "InputChannel.h"
#include "InputGate.h"
#include "checkpoint/channel/ChannelStateWriter.h"
// check
namespace omnistream {

    class IndexedInputGate : public InputGate, public CheckpointableInput {
    public:
        IndexedInputGate() = default;
        ~IndexedInputGate() override = default;

        virtual int GetGateIndex()  = 0;
        virtual std::vector<InputChannelInfo> getUnfinishedChannels()  = 0;

        int GetInputGateIndex() override;
        void CheckpointStarted(const CheckpointBarrier& barrier) override;
        void CheckpointStopped(long checkpointId) override;
        std::vector<InputChannelInfo> GetChannelInfos() override;
        void SetChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter);
        void BlockConsumption(const InputChannelInfo& channelInfo) override {   // checkinto related , do it ilater
        }
        void ConvertToPriorityEvent(int channelIndex, int sequenceNumber)  {   // checkinto related , do it ilater
            getChannel(channelIndex)->ConvertToPriorityEvent(sequenceNumber);
        }

        virtual int getBuffersInUseCount()  = 0;
        virtual void announceBufferSize(int bufferSize) = 0;

        std::string toString()  override {
            return "IndexedInputGate [gateIndex=" + std::to_string(GetGateIndex()) + "]";
        }
    };

} // namespace omnistream

#endif // OMNISTREAM_INDEXEDINPUTGATE_H