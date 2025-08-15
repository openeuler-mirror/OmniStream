/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_COMMITTABLEMESSAGE_H
#define FLINK_BENCHMARK_COMMITTABLEMESSAGE_H
#pragma once

#include <optional>
#include <cstdint>

/**
 * The message sent from SinkWriter to Committer.
 * This is an experimental interface.
 */
template<typename CommT>
class CommittableMessage {
public:
    virtual ~CommittableMessage() = default;

    /// @return The subtask that created this committable
    virtual int GetSubtaskId() const = 0;

    /**
     * @return The checkpoint id or empty if the message does not belong to a checkpoint.
     *         In that case, the committable was created at the end of input (e.g., in batch mode).
     */
    virtual std::optional<int64_t> GetCheckpointId() const = 0;
};
#endif // FLINK_BENCHMARK_COMMITTABLEMESSAGE_H
