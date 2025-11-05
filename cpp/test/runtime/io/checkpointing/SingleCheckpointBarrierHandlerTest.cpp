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
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "runtime/io/checkpointing/SingleCheckpointBarrierHandler.h"
#include "runtime/io/checkpointing/BarrierHandlerState.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrierUnaligned.h"
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/metrics/SystemClock.h"
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"
#include "runtime/io/checkpointing/AlternatingCollectingBarriersUnaligned.h"
#include "MockClasses.h"
#include "runtime/io/checkpointing/AlternatingWaitingForFirstBarrier.h"
#include "runtime/io/checkpointing/AlternatingCollectingBarriers.h"

using namespace omnistream;
using namespace omnistream::runtime;
using ::testing::Return;
using ::testing::_;
using ::testing::NiceMock;

class MockCheckpointableTask : public CheckpointableTask {
public:
    MOCK_METHOD(void, TriggerCheckpointOnBarrier,
        (CheckpointMetaData* checkpointMetaData,
            CheckpointOptions* checkpointOptions, CheckpointMetricsBuilder* checkpointMetrics), (override));

    MOCK_METHOD(void, abortCheckpointOnBarrier,
                (long checkpointId, CheckpointException cause), (override));
};

class MockSubtaskCheckpointCoordinator : public SubtaskCheckpointCoordinator {
public:
    MOCK_METHOD(void, InitInputsCheckpoint, (long id, CheckpointOptions * checkpointOptions), (override));
};

TEST(SingleCheckpointBarrierHandlerTest, ProcessBarrier_ValidBarrier_TransitionsStateAndMarksAlignment) {
    auto mockTask = std::make_unique<NiceMock<MockCheckpointableTask>>();
    auto mockCoordinator = std::make_unique<NiceMock<MockSubtaskCheckpointCoordinator>>();
    Clock& clock = SystemClock::GetInstance();
    auto mockInput = std::make_unique<TestInput>(0);

    InputChannelInfo channelInfo(0, 0);

    std::vector<CheckpointableInput *> inputs = {mockInput.get()};

    auto state = new AlternatingWaitingForFirstBarrierUnaligned(false, ChannelState(inputs));

    auto executor = new MailboxExecutorTest();
    auto timerService = new SystemProcessingTimeService();

    auto delayableTimer = BarrierAlignmentUtil::createRegisterTimerCallback<std::function<void()>>(
            executor, timerService);
    auto cancellable = delayableTimer->RegisterTask([]() {},
                                                    std::chrono::milliseconds(100));

    EXPECT_NE(cancellable, nullptr);
    EXPECT_NO_THROW(cancellable->Cancel());

    SingleCheckpointBarrierHandler *handler = new SingleCheckpointBarrierHandler(
            "testTask",
            mockTask.get(),
            mockCoordinator.get(),
            clock,
            1, // numOpenChannels
            state,
            false, // alternating
            delayableTimer,
            inputs,
            false // enableCheckpointAfterTasksFinished
    );

    auto checkpointType = CheckpointType::CHECKPOINT;
    auto targetLocation = CheckpointStorageLocationReference::GetDefault();
    CheckpointOptions *options = new CheckpointOptions(checkpointType, targetLocation);
    CheckpointBarrier barrier(1, 123456789, options);

    // ACT
    handler->ProcessBarrier(barrier, channelInfo, false);

    // ASSERT
    EXPECT_EQ(handler->GetLatestCheckpointId(), 1);
    EXPECT_FALSE(handler->IsCheckpointPending());

    BarrierHandlerState *next = state->BarrierReceived(handler->GetContext(), channelInfo, &barrier, true);

    auto *asAlignedWaiting = dynamic_cast<AlternatingCollectingBarriersUnaligned *>(next);
    EXPECT_NE(asAlignedWaiting, nullptr);
}

TEST(SingleCheckpointBarrierHandlerTest, AlignmentTimeout_SwitchesToUnaligned) {
    auto mockTask = std::make_unique<NiceMock<MockCheckpointableTask>>();
    auto mockCoordinator = std::make_unique<NiceMock<MockSubtaskCheckpointCoordinator>>();
    Clock& clock = SystemClock::GetInstance();

    // 2 input channels
    auto input0 = std::make_unique<TestInput>(0);
    auto input1 = std::make_unique<TestInput>(1);
    std::vector<CheckpointableInput*> inputs = {input0.get(), input1.get()};

    auto executor = new MailboxExecutorTest();
    auto timerService = std::make_shared<SystemProcessingTimeService>();

    auto delayableTimer = BarrierAlignmentUtil::createRegisterTimerCallback<std::function<void()>>(
        executor, timerService.get());

    // Start with aligned state
    auto* initialState = new AlternatingWaitingForFirstBarrier(ChannelState(inputs));

    auto* handler = new SingleCheckpointBarrierHandler(
        "JoinTask",
        mockTask.get(),
        mockCoordinator.get(),
        clock,
        2,  // two input channels
        initialState,
        true,  // is alternating
        delayableTimer,
        inputs,
        false // enableCheckpointAfterTasksFinished
    );

    // Barrier setup
    auto checkpointType = CheckpointType::CHECKPOINT;
    auto targetLocation = CheckpointStorageLocationReference::GetDefault();
    auto *options = new CheckpointOptions(checkpointType, targetLocation, CheckpointOptions::AlignmentType::ALIGNED, 3000);
    CheckpointBarrier barrier(42, clock.RelativeTimeMillis(), options);

    InputChannelInfo channel0(0, 0);
    InputChannelInfo channel1(1, 0);

    // Step 1: Process barrier on channel 0
    handler->ProcessBarrier(barrier, channel0, false);

    // Assert: Only one barrier received, alignment in progress
    EXPECT_EQ(handler->GetLatestCheckpointId(), 42);
    EXPECT_TRUE(handler->IsCheckpointPending());
    auto currentState = handler->GetCurrentState();
    auto* asAligned = dynamic_cast<AlternatingCollectingBarriers*>(currentState);
    EXPECT_NE(asAligned, nullptr) << "Expected Aligned mode";

    // Sleep for alignment timeout to trigger fallback to unaligned
    std::this_thread::sleep_for(std::chrono::milliseconds(3150));

    currentState = handler->GetCurrentState();
    auto* asUnaligned = dynamic_cast<AlternatingCollectingBarriersUnaligned*>(currentState);

    // Step 2: Process barrier on channel 1
    handler->ProcessBarrier(barrier, channel1, false);

    currentState = handler->GetCurrentState();
    auto *asAlignedAgain = dynamic_cast<AlternatingWaitingForFirstBarrier*>(currentState);
    EXPECT_NE(asAlignedAgain, nullptr) << "Expected Aligned mode";

    delete handler;
}
