/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_SUBTASKCOMMITTABLEMANAGER_H
#define FLINK_BENCHMARK_SUBTASKCOMMITTABLEMANAGER_H
#include <deque>
#include <memory>
#include <optional>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include "CommittableWithLineage.h"
#include "CommitRequestImpl.h"

template <typename CommT>
class SubtaskCommittableManager {
public:
    SubtaskCommittableManager(int numExpectedCommittables, int subtaskId, std::optional<long> checkpointId);

    explicit SubtaskCommittableManager(
            const std::vector<std::shared_ptr<CommitRequestImpl<CommT>>>& requests,
            int numExpectedCommittables,
            int numDrained,
            int numFailed,
            int subtaskId,
            std::optional<long> checkpointId);

    void Add(const CommittableWithLineage<CommT>& committable);

    void Add(const CommT& committable);

    bool HasReceivedAll() const;

    int GetNumCommittables() const;

    int GetNumPending() const;

    int GetNumFailed() const;

    bool IsFinished() const;

    std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> GetPendingRequests() const;

    std::vector<CommittableWithLineage<CommT>> DrainCommitted();

    int GetNumDrained() const;

    int GetSubtaskId() const;

    std::optional<long> GetCheckpointId() const;

    std::deque<std::shared_ptr<CommitRequestImpl<CommT>>> GetRequests() const;

    SubtaskCommittableManager<CommT> Merge(const SubtaskCommittableManager<CommT>& other);

    SubtaskCommittableManager<CommT> Copy() const;

private:
    std::deque<std::shared_ptr<CommitRequestImpl<CommT>>> requests;
    int numExpectedCommittables;
    std::optional<long> checkpointId;
    int subtaskId;
    int numDrained;
    int numFailed;
};
#endif // FLINK_BENCHMARK_SUBTASKCOMMITTABLEMANAGER_H
