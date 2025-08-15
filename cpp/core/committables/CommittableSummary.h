/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_COMMITTABLESUMMARY_H
#define FLINK_BENCHMARK_COMMITTABLESUMMARY_H

#include <optional>
#include <type_traits>
#include "CommittableMessage.h"

template <typename CommT>
class CommittableSummary : public CommittableMessage<CommT> {
public:
    CommittableSummary(
        int subtaskId,
        int numberOfSubtasks,
        std::optional<long> checkpointId,
        int numberOfCommittables,
        int numberOfPendingCommittables,
        int numberOfFailedCommittables)
        : subtaskId(subtaskId),
          numberOfSubtasks(numberOfSubtasks),
          checkpointId(checkpointId),
          numberOfCommittables(numberOfCommittables),
          numberOfPendingCommittables(numberOfPendingCommittables),
          numberOfFailedCommittables(numberOfFailedCommittables) {}

    int GetSubtaskId() const
    {
        return subtaskId;
    }

    int GetNumberOfSubtasks() const
    {
        return numberOfSubtasks;
    }

    std::optional<long> GetCheckpointId() const
    {
        return checkpointId;
    }

    int GetNumberOfCommittables() const
    {
        return numberOfCommittables;
    }

    int GetNumberOfPendingCommittables() const
    {
        return numberOfPendingCommittables;
    }

    int GetNumberOfFailedCommittables() const
    {
        return numberOfFailedCommittables;
    }
private:
    const int subtaskId;
    const int numberOfSubtasks;

    const std::optional<long> checkpointId;
    const int numberOfCommittables;
    const int numberOfPendingCommittables;
    const int numberOfFailedCommittables;
};

#endif // FLINK_BENCHMARK_COMMITTABLESUMMARY_H
