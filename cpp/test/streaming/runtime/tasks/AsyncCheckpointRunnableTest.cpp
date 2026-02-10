#include "streaming/runtime/tasks/AsyncCheckpointRunnable.h"
#include <gtest/gtest.h>

TEST(AsyncCheckpointRunnableTest, InitTest)
{
    auto asyncCheckpointRunnable = new AsyncCheckpointRunnable(
        new std::unordered_map<OperatorID, OperatorSnapshotFutures *>(),
        CheckpointMetaData(1, 1000), 
        CheckpointMetricsBuilder(),
        0, 
        "TestTask", 
        new std::function<void(AsyncCheckpointRunnable *)>(
            [](AsyncCheckpointRunnable *) {}), 
        nullptr, 
        new std::function<void(std::string, std::exception)>(
            [](std::string, std::exception) {}),
        false, 
        false, 
        std::make_shared<omnistream::LambdaSupplier<bool>>(std::function<std::shared_ptr<bool>()>(
            []() { return std::make_shared<bool>(true); })));
    EXPECT_TRUE(asyncCheckpointRunnable->IsRunning());
    delete asyncCheckpointRunnable;
}
