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
#include "SingleCheckpointBarrierHandler.h"
#include <iostream>
#include <algorithm>
#include <stdexcept>
#include "common.h"
#include "io/network/partition/consumer/CheckpointableInput.h"
#include "runtime/jobgraph/tasks/CheckpointableTask.h"
#include "runtime/io/checkpointing/BarrierAlignmentUtil.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.h"
#include "WaitingForFirstBarrier.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrier.h"

namespace omnistream::runtime {

SingleCheckpointBarrierHandler::SingleCheckpointBarrierHandler(
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    SubtaskCheckpointCoordinator* subTaskCheckpointCoordinator,
    Clock& clock,
    int numOpenChannels,
    BarrierHandlerState* currentState,
    bool alternating,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
    const std::vector<CheckpointableInput*>& inputs,
    bool enableCheckpointAfterTasksFinished)
    : CheckpointBarrierHandler(toNotifyOnCheckpoint, clock, enableCheckpointAfterTasksFinished),
      taskName_(taskName),
      registerTimer_(registerTimer),
      subTaskCheckpointCoordinator_(subTaskCheckpointCoordinator),
      inputs_(inputs),
      currentCheckpointId_(-1L),
      targetChannelCount_(0),
      lastCancelledOrCompletedCheckpointId_(-1L),
      numOpenChannels_(numOpenChannels),
      currentState_(currentState),
      alternating_(alternating)
{
    context_ = new ControllerImpl(this, subTaskCheckpointCoordinator_);
}

// Main ProcessBarrier implementation
void SingleCheckpointBarrierHandler::ProcessBarrier(
    const CheckpointBarrier& barrier,
    const InputChannelInfo& channelInfo,
    bool isRpcTriggered)
{
    int64_t barrierId = barrier.GetId();
    LOG(taskName_ + ": Received barrier from channel " + channelInfo.toString() + " @" + std::to_string(barrierId));
    if (currentCheckpointId_ > barrierId ||
        (currentCheckpointId_ == barrierId && !IsCheckpointPending())) {
        if (!barrier.GetCheckpointOptions()->IsUnalignedCheckpoint()) {
            inputs_[channelInfo.getGateIdx()]->ResumeConsumption(channelInfo);
        }
        return;
    }

    CheckNewCheckpoint(barrier);

    // Verify state consistency
    if (currentCheckpointId_ != barrierId) {
        throw std::runtime_error("Current checkpoint ID mismatch");
    }

    // Transform state with barrier received
    auto stateTransformer = [this, &channelInfo, barrier, isRpcTriggered](
        BarrierHandlerState *state) -> BarrierHandlerState* {
        LOG(">>>>> stateTransformer")
        return state->BarrierReceived(context_, channelInfo, const_cast<CheckpointBarrier*>(&barrier), !isRpcTriggered);
    };

    MarkCheckpointAlignedAndTransformState(channelInfo, barrier, stateTransformer);
}

// Mark checkpoint aligned and transform state
void SingleCheckpointBarrierHandler::MarkCheckpointAlignedAndTransformState(
    const InputChannelInfo& alignedChannel,
    const CheckpointBarrier& barrier,
    const std::function<BarrierHandlerState*(BarrierHandlerState*)>& stateTransformer)
{
    LOG(">>>>>")
    alignedChannels_.insert(alignedChannel);
    if (alignedChannels_.size() == 1) {
        if (targetChannelCount_ == 1) {
            MarkAlignmentStartAndEnd(barrier.GetId(), barrier.GetTimestamp());
        } else {
            MarkAlignmentStart(barrier.GetId(), barrier.GetTimestamp());
        }
    }

    // Mark alignment end before calling state transformer
    if (alignedChannels_.size() == (unsigned int)targetChannelCount_) {
        if (targetChannelCount_ > 1) {
            MarkAlignmentEnd();
        }
    }

    try {
        currentState_ = stateTransformer(currentState_);
    } catch (const CheckpointException& e) {
        AbortInternal(currentCheckpointId_, e);
        return;
    } catch (const std::exception& e) {
        // Convert to IOException equivalent
        throw std::runtime_error("Error in state transformation: " + std::string(e.what()));
    }

    if (alignedChannels_.size() == (unsigned int)targetChannelCount_) {
        alignedChannels_.clear();
        lastCancelledOrCompletedCheckpointId_ = currentCheckpointId_;
        LOG(taskName_ + ": All the channels are aligned for checkpoint " + std::to_string(currentCheckpointId_))

        ResetAlignmentTimer();
        allBarriersReceivedFuture_.Complete();
    }
}

// Trigger checkpoint - calls NotifyCheckpoint
void SingleCheckpointBarrierHandler::TriggerCheckpoint(const CheckpointBarrier& trigger)
{
    LOG(taskName_ + " : Triggering checkpoint " + std::to_string(trigger.GetId()) +
    " on the barrier announcement at " + std::to_string(trigger.GetTimestamp()));
    NotifyCheckpoint(trigger);
}

// Process barrier announcement
void SingleCheckpointBarrierHandler::ProcessBarrierAnnouncement(
    const CheckpointBarrier& announcedBarrier,
    int sequenceNumber,
    const InputChannelInfo& channelInfo)
{
        CheckNewCheckpoint(announcedBarrier);

    int64_t barrierId = announcedBarrier.GetId();
    if (currentCheckpointId_ > barrierId ||
        (currentCheckpointId_ == barrierId && !IsCheckpointPending())) {
        std::cout << taskName_ << ": Obsolete announcement of checkpoint " << barrierId
                  << " for channel " << channelInfo.toString() << std::endl;
        return;
    }
}

// Process cancellation barrier
void SingleCheckpointBarrierHandler::ProcessCancellationBarrier(
    const CancelCheckpointMarker& cancelBarrier,
    const InputChannelInfo& channelInfo)
{
    int64_t cancelledId = cancelBarrier.getCheckpointId();
    if (cancelledId > currentCheckpointId_ ||
        (cancelledId == currentCheckpointId_ && alignedChannels_.size() > 0)) {
        std::cout << taskName_ << ": Received cancellation " << cancelledId << std::endl;
        AbortInternal(cancelledId, CheckpointFailureReason::CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER);
    }
}

// Process end of partition
void SingleCheckpointBarrierHandler::ProcessEndOfPartition(const InputChannelInfo& channelInfo)
{
    numOpenChannels_--;
}

// Check new checkpoint
void SingleCheckpointBarrierHandler::CheckNewCheckpoint(const CheckpointBarrier& barrier)
{
    LOG(">>>>>" << " id " << barrier.GetId()<< " timestamp " << barrier.GetTimestamp())
    int64_t barrierId = barrier.GetId();
    if (currentCheckpointId_ >= barrierId) {
        return; // This barrier is not the first for this checkpoint
    }

    if (IsCheckpointPending()) {
        CancelSubsumedCheckpoint(barrierId);
    }

    currentCheckpointId_ = barrierId;
    pendingCheckpointBarrier_ = const_cast<CheckpointBarrier*>(&barrier);
    alignedChannels_.clear();
    targetChannelCount_ = numOpenChannels_;
    allBarriersReceivedFuture_ = CompletableFutureV2<void>();
}

// Register alignment timer
void SingleCheckpointBarrierHandler::RegisterAlignmentTimer(const CheckpointBarrier& announcedBarrier)
{
    // Assuming BarrierAlignmentUtil::getTimerDelay exists
    int64_t timerDelay = BarrierAlignmentUtil::getTimerDelay(GetClock().RelativeTimeMillis(), announcedBarrier);

    auto barrierId = announcedBarrier.GetId();
    auto timerTask = [this, announcedBarrier, barrierId]() {
        try {
            if (currentCheckpointId_ == barrierId && !GetAllBarriersReceivedFuture(barrierId).IsDone()) {
                currentState_ = currentState_->AlignedCheckpointTimeout(
                    dynamic_cast<Controller*>(context_),
                    const_cast<CheckpointBarrier*>(&announcedBarrier));
            }
        } catch (const CheckpointException& ex) {
            AbortInternal(barrierId, ex);
        } catch (const std::exception& e) {
            throw std::runtime_error("Timer task error: " + std::string(e.what()));
        }
        currentAlignmentTimer_.reset();
    };
    currentAlignmentTimer_.reset(registerTimer_->RegisterTask(timerTask, std::chrono::milliseconds(timerDelay)));
}

// Abort internal with reason
void SingleCheckpointBarrierHandler::AbortInternal(int64_t cancelledId, CheckpointFailureReason reason)
{
    CheckpointException exception(reason);
    AbortInternal(cancelledId, exception);
}

// Abort internal with exception
void SingleCheckpointBarrierHandler::AbortInternal(int64_t cancelledId, const CheckpointException& exception) {}

// Cancel subsumed checkpoint
void SingleCheckpointBarrierHandler::CancelSubsumedCheckpoint(int64_t barrierId)
{
    std::cout << taskName_ << ": Received checkpoint barrier for checkpoint " << barrierId
              << " before completing current checkpoint " << currentCheckpointId_
              << ". Skipping current checkpoint." << std::endl;
    LOG(taskName_ + ": Received checkpoint barrier for checkpoint " + std::to_string(barrierId) +
    " before completing current checkpoint " + std::to_string(currentCheckpointId_) + ". Skipping current checkpoint.")
    AbortInternal(currentCheckpointId_, CheckpointFailureReason::CHECKPOINT_DECLINED_SUBSUMED);
}

// Reset alignment timer
void SingleCheckpointBarrierHandler::ResetAlignmentTimer()
{
    if (currentAlignmentTimer_) {
        currentAlignmentTimer_->Cancel();
        currentAlignmentTimer_.reset();
    }
}
/*
// Reset all barriers received future
void SingleCheckpointBarrierHandler::ResetAllBarriersReceivedFuture()
{
    allBarriersReceivedPromise_ = std::promise<void>();
    allBarriersReceivedFuture_ = allBarriersReceivedPromise_.get_future();
}
*/
// Get latest checkpoint ID
int64_t SingleCheckpointBarrierHandler::GetLatestCheckpointId() const
{
    return currentCheckpointId_;
}

BarrierHandlerState *SingleCheckpointBarrierHandler::GetCurrentState() const
{
    return currentState_;
}

// Check if checkpoint is pending
bool SingleCheckpointBarrierHandler::IsCheckpointPending() const
{
    return currentCheckpointId_ != lastCancelledOrCompletedCheckpointId_ && currentCheckpointId_ >= 0;
}

// Get all barriers received future
CompletableFutureV2<void>& SingleCheckpointBarrierHandler::GetAllBarriersReceivedFuture(int64_t checkpointId)
{
    if (checkpointId < currentCheckpointId_ || numOpenChannels_ == 0) {
        if (!completed.IsDone()) {
            completed.Complete();
        }
        return completed;
    }

    if (checkpointId > currentCheckpointId_) {
        throw std::runtime_error("Checkpoint " + std::to_string(checkpointId) + " has not been started at all");
    }
    return allBarriersReceivedFuture_;
}

// Get number of open channels
int SingleCheckpointBarrierHandler::GetNumOpenChannels() const
{
    return numOpenChannels_;
}

// Close handler
void SingleCheckpointBarrierHandler::Close()
{
    ResetAlignmentTimer();
    allBarriersReceivedFuture_.Cancel();
    CheckpointBarrierHandler::Close();
}

// ToString method
std::string SingleCheckpointBarrierHandler::ToString() const
{
    return taskName_ + ": current checkpoint: " + std::to_string(currentCheckpointId_) +
           ", current aligned channels: " + std::to_string(alignedChannels_.size()) +
           ", target channel count: " + std::to_string(targetChannelCount_);
}

// Factory methods - simplified implementations
std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::createUnalignCheckpointBarrierHandler(
    SubtaskCheckpointCoordinator* checkpointCoordinator,
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    Clock& clock,
    bool enableCheckpointsAfterTasksFinish,
    const std::vector<CheckpointableInput*>& inputs)
{
    // Calculate total channel count
    int channelCount = 0;
    for (const auto& input : inputs) {
        channelCount += input->GetChannelInfos().size();
    }

    // Create a simple timer that throws for unaligned checkpoints
    auto timer = std::make_unique<BarrierAlignmentUtil::ThrowingDelayableTimer<std::function<void()>>>();
    return unaligned(taskName, toNotifyOnCheckpoint, checkpointCoordinator,
        clock, channelCount, timer.get(), enableCheckpointsAfterTasksFinish, inputs);
}

std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::unaligned(
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint,
    SubtaskCheckpointCoordinator* checkpointCoordinator,
    Clock& clock,
    int numOpenChannels,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
    bool enableCheckpointAfterTasksFinished,
    const std::vector<CheckpointableInput*>& inputs)
{
    auto channelState = new ChannelState(inputs);
    // Create unaligned state - this would need to be implemented
    auto state = new AlternatingWaitingForFirstBarrierUnaligned(false, *channelState);

    return std::make_unique<SingleCheckpointBarrierHandler>(
        taskName, toNotifyOnCheckpoint, checkpointCoordinator, clock, numOpenChannels,
        state, true, registerTimer, inputs, enableCheckpointAfterTasksFinished);
}

std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::aligned(const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint, Clock& clock, int numOpenChannels,
    BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer, bool enableCheckpointAfterTasksFinished,
    const std::vector<CheckpointableInput*>& inputs)
{
    LOG(">>>>>>>>>>>")
    auto state = new WaitingForFirstBarrier();
    // fix state waitingforfirstbarrier is implemented
    return std::make_unique<SingleCheckpointBarrierHandler>(
            taskName, toNotifyOnCheckpoint, nullptr, clock, numOpenChannels,
            state, false, registerTimer, inputs, enableCheckpointAfterTasksFinished);
}

std::unique_ptr<SingleCheckpointBarrierHandler> SingleCheckpointBarrierHandler::alternating(
    const std::string& taskName,
    CheckpointableTask* toNotifyOnCheckpoint, SubtaskCheckpointCoordinator* checkpointCoordinator, Clock& clock,
    int numOpenChannels, BarrierAlignmentUtil::DelayableTimer<std::function<void()>>* registerTimer,
    bool enableCheckpointAfterTasksFinished, const std::vector<CheckpointableInput*>& inputs)
{
    LOG(">>>>>>>>>>")
    auto channelState = new ChannelState(inputs);
    auto state = new AlternatingWaitingForFirstBarrier(*channelState);

    return std::make_unique<SingleCheckpointBarrierHandler>(
            taskName, toNotifyOnCheckpoint, checkpointCoordinator, clock, numOpenChannels,
            state, true, registerTimer, inputs, enableCheckpointAfterTasksFinished);
}
} // omnistream