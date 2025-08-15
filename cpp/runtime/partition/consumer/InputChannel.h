/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// InputChannel.h
#ifndef INPUT_CHANNEL_H
#define INPUT_CHANNEL_H

#include <memory>
#include <atomic>
#include <optional>
#include <stdexcept>
#include <event/TaskEvent.h>
#include <metrics/Counter.h>

#include "BufferAndAvailability.h"
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>
#include "InputChannelInfo.h"
#include "partition/virtual_enable_shared_from_this_base.h"

namespace omnistream {
class SingleInputGate;

class InputChannel : public virtual_enable_shared_from_this<InputChannel> {
protected:
    InputChannelInfo channelInfo;
    ResultPartitionIDPOD partitionId;
    std::shared_ptr<SingleInputGate> inputGate;
    // std::atomic<std::shared_ptr<std::exception>> cause;
    std::atomic<bool> exception_occurred{false};
    std::exception_ptr cause;
    std::mutex exception_mutex;

    int initialBackoff;
    int maxBackoff;
    std::shared_ptr<Counter> numBytesIn;
    std::shared_ptr<Counter> numBuffersIn;
    int currentBackoff;

public:
    InputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex, ResultPartitionIDPOD partitionId,
        int initialBackoff, int maxBackoff, std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn);

    int getChannelIndex() const;
    InputChannelInfo getChannelInfo() const;
    ResultPartitionIDPOD getPartitionId() const;

    virtual void resumeConsumption() = 0;
    virtual void acknowledgeAllRecordsProcessed() = 0;

protected:
    void notifyChannelNonEmpty();
    void notifyPriorityEvent(int priorityBufferNumber);
    virtual void notifyBufferAvailable(int numAvailableBuffers)
    {}

public:
    virtual void requestSubpartition(int subpartitionIndex) = 0;
    virtual std::optional<BufferAndAvailability> getNextBuffer() = 0;

    // virtual void checkpointStarted(std::shared_ptr<CheckpointBarrier> barrier) {}
    virtual void checkpointStopped(long checkpointId)
    {}
    virtual void convertToPriorityEvent(int sequenceNumber)
    {}

    virtual void sendTaskEvent(std::shared_ptr<TaskEvent> event) = 0;

    virtual bool isReleased() = 0;
    virtual void releaseAllResources() = 0;
    virtual void announceBufferSize(int newBufferSize) = 0;
    virtual int getBuffersInUseCount() = 0;

public:
    void checkError();
    void setError(std::exception_ptr cause);

    int getCurrentBackoff() const;
    bool increaseBackoff();

public:
    virtual int unsynchronizedGetNumberOfQueuedBuffers()
    {
        return 0;
    }
    virtual void setup(){};
    virtual std::string toString() = 0;
    static const int initBackoffConstant = 100;
    static const int maxBackoffConstant = 1000;
};

}  // namespace omnistream

#endif  // INPUT_CHANNEL_H