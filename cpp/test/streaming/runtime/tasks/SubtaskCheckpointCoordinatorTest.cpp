#include <gtest/gtest.h>
#include "streaming/runtime/tasks/SubtaskCheckpointCoordinatorImpl.h"
using namespace omnistream::runtime;
TEST(SubtaskCheckpointCoordinatorTest, InitTest)
{
    auto coordinator = new SubtaskCheckpointCoordinatorImpl(
        nullptr,
        "tempName",
        std::make_shared<omnistream::ImmediateStreamTaskActionExecutor>(),
        std::make_shared<omnistream::RuntimeEnvironmentV2>(),
        false,
        false,
        new std::function<CompletableFutureV2<void> *(ChannelStateWriter *, long)>(),
        new BarrierAlignmentUtil::DelayableTimer<std::function<void()>>());
    delete coordinator;
}