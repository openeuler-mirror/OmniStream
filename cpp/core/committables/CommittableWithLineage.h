/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_COMMITTABLEWITHLINEAGE_H
#define FLINK_BENCHMARK_COMMITTABLEWITHLINEAGE_H
#include <optional>
#include <utility>
#include <stdexcept>
#include "CommittableMessage.h"

// 假设 CommittableMessage 和其他相关类已经从其他文件中引用
template <typename CommT>
class CommittableWithLineage : public CommittableMessage<CommT> {
public:
    CommittableWithLineage(CommT committable, std::optional<long> checkpointId, int subtaskId)
        : committable(std::move(committable)), checkpointId(checkpointId), subtaskId(subtaskId)
    {
        if (!committable) {
            throw std::invalid_argument("committable must not be null");
        }
    }

    CommT GetCommittable() const
    {
        return committable;
    }

    int GetSubtaskId() const
    {
        return subtaskId;
    }

    std::optional<long> GetCheckpointId() const
    {
        return checkpointId;
    }
private:
    CommT committable;
    std::optional<long> checkpointId;
    int subtaskId;
};
#endif // FLINK_BENCHMARK_COMMITTABLEWITHLINEAGE_H
