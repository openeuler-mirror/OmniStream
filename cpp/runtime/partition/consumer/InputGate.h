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
#include "partition/ChannelStateHolder.h"
#include <event/TaskEvent.h>

// check
namespace omnistream {

class InputGate : public PullingAsyncDataInput<BufferOrEvent>, public ChannelStateHolder {
public:
    InputGate() = default;
    virtual ~InputGate() = default;

    void setChannelStateWriter(std::shared_ptr<ChannelStateWriter> channelStateWriter) override;

    virtual int GetNumberOfInputChannels() = 0;
    virtual bool IsFinished() = 0;
    virtual bool HasReceivedEndOfData() = 0;
    virtual BufferOrEvent* GetNext() = 0;
    // BufferOrEvent* PollNext() override = 0;
    virtual void sendTaskEvent(const std::shared_ptr<TaskEvent>& event) = 0;

    std::shared_ptr<CompletableFuture> GetAvailableFuture() override;

    virtual void ResumeConsumption(const InputChannelInfo& channelInfo) = 0;
    virtual void acknowledgeAllRecordsProcessed(const InputChannelInfo& channelInfo) = 0;
    virtual std::shared_ptr<InputChannel> getChannel(int channelIndex) = 0;

    std::vector<InputChannelInfo> getChannelInfos();
    bool fromOriginal();

    std::shared_ptr<CompletableFuture> getPriorityEventAvailableFuture();

    virtual void setup() = 0;
    virtual void RequestPartitions() = 0;
    virtual std::shared_ptr<CompletableFuture> getStateConsumedFuture() = 0;
    virtual void FinishReadRecoveredState() = 0;

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
