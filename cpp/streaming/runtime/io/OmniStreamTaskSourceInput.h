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

#ifndef OMNISTREAM_OMNISTREAMTASKSOURCEINPUT_H
#define OMNISTREAM_OMNISTREAMTASKSOURCEINPUT_H

#include "OmniStreamTaskInput.h"
#include "streaming/api/operators/SourceOperator.h"
#include "runtime/io/AvailabilityHelper.h"
#include "partition/consumer/InputChannelInfo.h"

namespace omnistream {

    class OmniStreamTaskSourceInput : public OmniStreamTaskInput, public CheckpointableInput {
    public:
        explicit OmniStreamTaskSourceInput(StreamOperator* sourceOperator, int inputGateIndex, int inputIndex)
            :sourceOperator(reinterpret_cast<SourceOperator<>*>(sourceOperator)), inputGateIndex(inputGateIndex), inputIndex(inputIndex)
        {
            inputChannelInfos.emplace_back(inputGateIndex, 0);
            isBlockedAvailability->resetAvailable();
        }

        DataInputStatus emitNext(OmniPushingAsyncDataInput::OmniDataOutput *output) override
        {
//            LOG("OmniSourceOperatorStreamTask::processInput")
            if (isBlockedAvailability->isApproximatelyAvailable()) {
                return sourceOperator->emitNext(output);
            }
            return DataInputStatus::NOTHING_AVAILABLE;
        }

        std::shared_ptr<CompletableFuture> GetAvailableFuture() override
        {
            // no inputGate no output
            return isBlockedAvailability->and_(sourceOperator->GetAvailableFuture());
        }

        int getInputIndex() override
        {
            return static_cast<int>(inputIndex);
        }

        StreamOperator* getOperator()
        {
            return sourceOperator;
        }

        void BlockConsumption(const InputChannelInfo& channelInfo) override {}

        void ResumeConsumption(const InputChannelInfo& channelInfo) override {}

        void ConvertToPriorityEvent(int channelIndex, int sequenceNumber) override {}

        void CheckpointStarted(const CheckpointBarrier& barrier) override {};

        void CheckpointStopped(long checkpointId) override {};

        std::vector<InputChannelInfo> GetChannelInfos() override
        {
            return inputChannelInfos;
        };

        int GetInputGateIndex() override
        {
            return inputGateIndex;
        }
    private:
        SourceOperator<>* sourceOperator;
        std::shared_ptr<AvailabilityHelper> isBlockedAvailability = std::make_shared<AvailabilityHelper>();
        int inputGateIndex;
        int inputIndex;
        std::vector<InputChannelInfo> inputChannelInfos;
    };
}


#endif // OMNISTREAM_OMNISTREAMTASKSOURCEINPUT_H
