/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGER_H
#define FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGER_H

#include <memory>
#include <optional>
#include <vector>
#include <stdexcept>
#include "CommittableManager.h"

template <typename CommT>
class CheckpointCommittableManager : public CommittableManager<CommT> {
public:
    virtual ~CheckpointCommittableManager() = default;

    /**
     * Returns the checkpoint id in which the committable was created.
     *
     * @return checkpoint id
     */
    virtual long GetCheckpointId() const = 0;
};

#endif // FLINK_BENCHMARK_CHECKPOINTCOMMITTABLEMANAGER_H
