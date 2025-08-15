/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by q00649235 on 2025/3/26.
//

#ifndef OMNISTREAM_OMNISTREAMTASKSOURCEINPUT_H
#define OMNISTREAM_OMNISTREAMTASKSOURCEINPUT_H

#include "OmniStreamTaskInput.h"
#include "streaming/api/operators/SourceOperator.h"
#include "runtime/io/AvailabilityHelper.h"
#include "partition/consumer/InputChannelInfo.h"

namespace omnistream {
    class OmniStreamTaskSourceInput : public OmniStreamTaskInput {
    private:
        SourceOperator<>* sourceOperator;
        std::shared_ptr<AvailabilityHelper> isBlockedAvailability = std::make_shared<AvailabilityHelper>();
        int inputGateIndex;
        int inputIndex;
        std::vector<InputChannelInfo> inputChannelInfos;

    public:
        explicit OmniStreamTaskSourceInput(StreamOperator* sourceOperator, int inputGateIndex, int inputIndex) :
            sourceOperator(reinterpret_cast<SourceOperator<>*>(sourceOperator)), inputGateIndex(inputGateIndex), inputIndex(inputIndex)
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

        std::shared_ptr<CompletableFuture> getAvailableFuture() override
        {
            // no inputGate no output
            return isBlockedAvailability->and_(sourceOperator->getAvailableFuture());
        }

        int getInputIndex() override
        {
            return static_cast<int>(inputIndex);
        }

        StreamOperator* getOperator()
        {
            return sourceOperator;
        }
    };

}


#endif // OMNISTREAM_OMNISTREAMTASKSOURCEINPUT_H
