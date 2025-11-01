#include "streaming/api/environment/ExecutionCheckpointingOptions.h"
#include <gtest/gtest.h>

TEST(ExecutionCheckpointingOptionsTest, StaticInstanceTest)
{
    EXPECT_EQ(ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->key(), "execution.checkpointing.checkpoints-after-tasks-finish.enabled");
    EXPECT_EQ(ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->hasDefaultValue(), true);
    EXPECT_EQ(ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->defaultValue(), false);
    EXPECT_EQ(ExecutionCheckpointingOptions::ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH->hasFallbackKeys(), false);

    EXPECT_EQ(ExecutionCheckpointingOptions::ALIGNED_CHECKPOINT_TIMEOUT->hasFallbackKeys(), true);
}