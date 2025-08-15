/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGERIMPL_H
#define FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGERIMPL_H

#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <stdexcept>
#include <functional>
#include <algorithm>
#include <iostream>
#include "CommittableMessage.h"
#include "CommittableSummary.h"
#include "CommittableWithLineage.h"
#include "CheckpointCommittableManager.h"
#include "SubtaskCommittableManager.h"
#include "CommitRequestImpl.h"

template <typename CommT>
class CheckpointCommittableManagerImpl : public CheckpointCommittableManager<CommT> {
public:
    using SubtaskCommittableManagers = std::map<int, std::shared_ptr<SubtaskCommittableManager<CommT>>>;

    CheckpointCommittableManagerImpl(int subtaskId, int numberOfSubtasks, std::optional<long> checkpointId);

    CheckpointCommittableManagerImpl(
            const SubtaskCommittableManagers& subtasksCommittableManagers,
            int subtaskId,
            int numberOfSubtasks,
            std::optional<long> checkpointId);

    long GetCheckpointId() const;

    std::vector<std::shared_ptr<SubtaskCommittableManager<CommT>>> getSubCM() const;

    void UpsertSummary(const CommittableSummary<CommT>& summary);

    void AddCommittable(const CommittableWithLineage<CommT>& committable);

    std::shared_ptr<SubtaskCommittableManager<CommT>> getSubtaskCommittableManager(int subtaskId) const;

    CommittableSummary<CommT> GetSummary() const;

    bool IsFinished() const;

    std::vector<CommittableWithLineage<CommT>> commit(
            bool fullyReceived,
            Committer<CommT>& committer);

    std::vector<CommitRequestImpl<CommT>> getPendingRequests(bool fullyReceived) const;

    std::vector<CommittableWithLineage<CommT>> drainFinished();

    CheckpointCommittableManagerImpl<CommT> Merge(const CheckpointCommittableManagerImpl<CommT>& other);

    CheckpointCommittableManagerImpl<CommT> Copy() const;

private:
    SubtaskCommittableManagers subtasksCommittableManagers;
    std::optional<long> checkpointId;
    int subtaskId;
    int numberOfSubtasks;
};

#endif // FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGERIMPL_H
