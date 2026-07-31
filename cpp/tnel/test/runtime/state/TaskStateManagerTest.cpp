#include <gtest/gtest.h>
#include "runtime/state/TaskStateManager.cpp"
#include "runtime/state/LocalRecoveryDirectoryProviderImpl.h"
using namespace omnistream;
TEST(TaskStateManagerTest, ReportAndComplete)
{
    JobIDPOD jobId;
    ExecutionAttemptIDPOD attemptId;
    omnistream::JobVertexID vertexId(0, 0);
    const std::vector<std::filesystem::path> path = {"/tmp"};
    auto dirProvider = std::make_shared<LocalRecoveryDirectoryProviderImpl>(path, jobId, vertexId, 0);
    auto localRecoveryConfig = std::make_shared<LocalRecoveryConfig>(dirProvider);
    auto stateStore = new TaskLocalStateStore(jobId, vertexId, 0, localRecoveryConfig);
    auto responder = new NoOpCheckpoingResponder();
    auto manager = new TaskStateManager(jobId, attemptId, stateStore, responder);
    auto metrics = new CheckpointMetrics(1, 1, 1, 1, 1, 1, false, 1, 1);
    auto snapshot = std::make_shared<TaskStateSnapshot>(4, true);
    auto metadata = new CheckpointMetaData(0, 100);

    manager->ReportTaskStateSnapshots(metadata, metrics, nullptr, snapshot);

    EXPECT_EQ(stateStore->retrieveLocalState(0)->ToString(), snapshot->ToString());

    manager->NotifyCheckpointComplete(1);

    EXPECT_EQ(stateStore->retrieveLocalState(0), nullptr);

    delete manager;
    delete metadata;
    delete metrics;
}

TEST(TaskStateManagerTest, ReportAndAbort)
{
    JobIDPOD jobId;
    ExecutionAttemptIDPOD attemptId;
    omnistream::JobVertexID vertexId(0, 0);
    const std::vector<std::filesystem::path> path = {"/tmp"};
    auto dirProvider = std::make_shared<LocalRecoveryDirectoryProviderImpl>(path, jobId, vertexId, 0);
    auto localRecoveryConfig = std::make_shared<LocalRecoveryConfig>(dirProvider);
    auto stateStore = new TaskLocalStateStore(jobId, vertexId, 0, localRecoveryConfig);
    auto responder = new NoOpCheckpoingResponder();
    auto manager = new TaskStateManager(jobId, attemptId, stateStore, responder);
    auto metrics = new CheckpointMetrics(1, 1, 1, 1, 1, 1, false, 1, 1);
    auto snapshot = std::make_shared<TaskStateSnapshot>(4, true);
    auto metadata = new CheckpointMetaData(0, 100);

    manager->ReportTaskStateSnapshots(metadata, metrics, nullptr, snapshot);

    EXPECT_EQ(*stateStore->retrieveLocalState(0), *snapshot);

    manager->NotifyCheckpointAborted(1);

    EXPECT_EQ(*stateStore->retrieveLocalState(0), *snapshot);

    manager->NotifyCheckpointAborted(0);

    EXPECT_EQ(stateStore->retrieveLocalState(0), nullptr);

    delete manager;
    delete metadata;
    delete metrics;
}
