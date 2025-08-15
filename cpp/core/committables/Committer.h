/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_COMMITTER_H
#define FLINK_BENCHMARK_COMMITTER_H

#include <vector>
#include <exception>
#include <stdexcept>
#include <memory>
#include "sink/KafkaCommittable.h"

/**
 * A request to Commit a specific committable.
 *
 * @param <CommT>
 */
template <typename CommT>
class CommitRequest {
public:
    virtual ~CommitRequest() = default;

    /**
     * Returns the committable.
     */
    virtual CommT GetCommittable() const = 0;

    /**
     * Returns how many times this particular committable has been retried. Starts at 0 for the
     * first attempt.
     */
    virtual int GetNumberOfRetries() const = 0;

    /**
     * The Commit failed for known reason and should not be retried.
     */
    virtual void signalFailedWithKnownReason(const std::exception& t) = 0;

    /**
     * The Commit failed for unknown reason and should not be retried.
     */
    virtual void signalFailedWithUnknownReason(const std::exception& t) = 0;

    /**
     * The Commit failed for a retriable reason. If the sink supports a retry maximum, this may
     * permanently fail after reaching that maximum. Else the committable will be retried as
     * long as this method is invoked after each attempt.
     */
    virtual void RetryLater() = 0;

    /**
     * Updates the underlying committable and retries later. This method can be used if a
     * committable partially succeeded.
     */
    virtual void UpdateAndRetryLater(const CommT& committable) = 0;

    /**
     * Signals that a committable is skipped as it was committed already in a previous run.
     */
    virtual void SignalAlreadyCommitted() = 0;
};

/**
 * The Committer is responsible for committing the data staged by the SinkWriter in the second step
 * of a two-phase Commit protocol.
 *
 * @param <CommT> The type of information needed to Commit the staged data
 */
template <typename CommT>
class Committer {
public:
    virtual ~Committer() = default;

    /**
     * Commit the given list of CommitRequests.
     *
     * @param committables A list of Commit requests staged by the sink writer.
     * @throws std::exception for reasons that may yield a complete restart of the job.
     */
    virtual void Commit(std::vector<CommitRequest<KafkaCommittable>> &committables) = 0;

    /**
     * Close the committer.
     */
    virtual void Close() = 0;
};
#endif // FLINK_BENCHMARK_COMMITTER_H
