/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// LocalInputChannel.h
#ifndef LOCALINPUTCHANNEL_H
#define LOCALINPUTCHANNEL_H

#include <memory>
#include <vector>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <partition/BufferAvailabilityListener.h>
#include <partition/ResultPartitionManager.h>
#include "partition/virtual_enable_shared_from_this_base.h"

#include "InputChannel.h"

#include "SingleInputGate.h"

namespace omnistream {

class LocalInputChannel : public InputChannel,
                          public BufferAvailabilityListener
// public std::enable_shared_from_this<LocalInputChannel>
{
public:
    LocalInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex, ResultPartitionIDPOD partitionId,
        std::shared_ptr<ResultPartitionManager> partitionManager,
        //   std::shared_ptr<TaskEventPublisher> taskEventPublisher,
        int initialBackoff, int maxBackoff, std::shared_ptr<Counter> numBytesIn, std::shared_ptr<Counter> numBuffersIn
        //  std::shared_ptr<ChannelStateWriter> stateWriter
    );

    //  void checkpointStarted(const CheckpointBarrier& barrier) override;
    void checkpointStopped(long checkpointId) override;
    void notifyDataAvailable() override;
    void resumeConsumption() override;
    void acknowledgeAllRecordsProcessed() override;
    bool isReleased() override;
    void releaseAllResources() override;
    void announceBufferSize(int newBufferSize) override;
    int getBuffersInUseCount() override;
    int unsynchronizedGetNumberOfQueuedBuffers() override;
    std::string toString() override;
    std::shared_ptr<ResultSubpartitionView> getSubpartitionView();
    void notifyBufferAvailable(int subpartitionId) override;

protected:
    void requestSubpartition(int subpartitionIndex) override;
    std::optional<BufferAndAvailability> getNextBuffer() override;
    void sendTaskEvent(std::shared_ptr<TaskEvent> event) override;

private:
    std::recursive_mutex requestLock;
    std::shared_ptr<ResultPartitionManager> partitionManager;
    //  std::shared_ptr<TaskEventPublisher> taskEventPublisher;
    std::shared_ptr<ResultSubpartitionView> subpartitionView;
    std::atomic<bool> isReleased_{false};
    //   ChannelStatePersister channelStatePersister;

public:
    void retriggerSubpartitionRequest(
        std::shared_ptr<std::chrono::steady_clock::time_point> timer, int subpartitionIndex);
    std::shared_ptr<ResultSubpartitionView> checkAndWaitForSubpartitionView();
};

}  // namespace omnistream

#endif  // LOCALINPUTCHANNEL_H