/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// InputGate.h
#ifndef OMNISTREAM_INPUTGATE_H
#define OMNISTREAM_INPUTGATE_H

#include <vector>
#include <memory>
#include <optional>
#include <future>
#include <iostream>
#include <io/PullingAsyncDataInput.h>

#include <io/AvailabilityHelper.h>
#include "BufferOrEvent.h"
#include "InputChannel.h"

#include "InputChannelInfo.h"
#include <event/TaskEvent.h>


namespace omnistream {

class InputGate : public PullingAsyncDataInput<BufferOrEvent> {
public:
    virtual ~InputGate() = default;

    virtual int getNumberOfInputChannels() = 0;
    virtual bool isFinished() = 0;
    virtual bool hasReceivedEndOfData() = 0;
    virtual  std::optional<std::shared_ptr<BufferOrEvent>> getNext() = 0;
    std::optional<std::shared_ptr<BufferOrEvent>> pollNext() override = 0;
    virtual void sendTaskEvent(const std::shared_ptr<TaskEvent>& event) = 0;

    std::shared_ptr<CompletableFuture> getAvailableFuture() override;

    virtual void resumeConsumption(const InputChannelInfo& channelInfo) = 0;
    virtual void acknowledgeAllRecordsProcessed(const InputChannelInfo& channelInfo) = 0;
    virtual std::shared_ptr<InputChannel> getChannel(int channelIndex) = 0;

    std::vector<InputChannelInfo> getChannelInfos();

    std::shared_ptr<CompletableFuture> getPriorityEventAvailableFuture();

    virtual void setup() = 0;
    virtual void requestPartitions() = 0;
    virtual std::shared_ptr<CompletableFuture> getStateConsumedFuture() = 0;
    virtual void finishReadRecoveredState() = 0;

protected:
    AvailabilityHelper availabilityHelper;
    AvailabilityHelper priorityAvailabilityHelper;

    template <typename INPUT, typename DATA>
    struct InputWithData {
        std::shared_ptr<INPUT> input;
        std::shared_ptr<DATA> data;
        bool moreAvailable;
        bool morePriorityEvents;

        InputWithData(std::shared_ptr<INPUT> input, std::shared_ptr<DATA> data, bool moreAvailable, bool morePriorityEvents);

        friend std::ostream& operator<<(std::ostream& os, const InputWithData& obj)
        {
            os << "InputWithData{input=" << obj.input << ", data=" << obj.data
               << ", moreAvailable=" << obj.moreAvailable << ", morePriorityEvents=" << obj.morePriorityEvents << "}";
            return os;
        }
    };
};
} // namespace omnistream

#endif // OMNISTREAM_INPUTGATE_H
