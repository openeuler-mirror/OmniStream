/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_COMMITTABLEMANAGER_H
#define FLINK_BENCHMARK_COMMITTABLEMANAGER_H
#include <memory>
#include <vector>
#include <exception>
#include <stdexcept>
#include "CommittableSummary.h"
#include "CommittableWithLineage.h"
#include "Committer.h"

/**
 * Internal wrapper to handle the committing of committables.
 *
 * @param <CommT> type of the committable
 */
template <typename CommT>
class CommittableManager {
public:
    virtual ~CommittableManager() = default;

    /**
     * Returns a summary of the current Commit progress.
     *
     * @return CommittableSummary
     */
    virtual CommittableSummary<CommT> GetSummary() const = 0;

    /**
     * Commits all due committables.
     *
     * @param fullyReceived only Commit committables if all committables of this checkpoint for a subtask are received
     * @param committer used to Commit to the external system
     * @return successfully committed committables with meta information
     * @throws std::exception
     */
    virtual std::vector<CommittableWithLineage<CommT>> commit(
            bool fullyReceived,
            Committer<CommT>& committer) = 0;
};
#endif // FLINK_BENCHMARK_COMMITTABLEMANAGER_H
