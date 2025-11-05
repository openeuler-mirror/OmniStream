/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
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
