#include <gtest/gtest.h>
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinatorImpl.h"
using namespace omnistream::runtime;
TEST(SubtaskCheckpointCoordinatorTest, InitTest)
{
    auto coordinator = std::make_shared<SubtaskCheckpointCoordinatorImpl>(
        nullptr,
        nullptr,
        "tempName",
        std::make_shared<omnistream::ImmediateStreamTaskActionExecutor>(),
        std::make_shared<omnistream::RuntimeEnvironmentV2>(),
        false,
        false,
        new std::function<std::shared_ptr<CompletableFutureV2<void>>(std::shared_ptr<ChannelStateWriter>, long)>(),
        new BarrierAlignmentUtil::DelayableTimer<std::function<void()>>());
}