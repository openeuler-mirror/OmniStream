/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_COMMITTABLECOLLECTOR_H
#define FLINK_BENCHMARK_COMMITTABLECOLLECTOR_H

#include <map>
#include <memory>
#include <optional>
#include <vector>
#include <stdexcept>
#include <algorithm>
#include <functional>
#include <iostream>
#include "CommittableMessage.h"
#include "CommittableSummary.h"
#include "CommittableWithLineage.h"
#include "CheckpointCommittableManagerImpl.h"

template <typename CommT>
class CommittableCollector {
public:
    using CheckpointCommittableMap = std::map<long, std::shared_ptr<CheckpointCommittableManagerImpl<CommT>>>;

    CommittableCollector(int subtaskId, int numberOfSubtasks);

    CommittableCollector(const CheckpointCommittableMap& checkpointCommittables, int subtaskId, int numberOfSubtasks);

    static CommittableCollector<CommT> Of(int subtaskId, int numberOfSubtasks);

    static CommittableCollector<CommT> ofLegacy(const std::vector<CommT>& committables);

    void AddMessage(const CommittableMessage<CommT>& message);

    std::vector<std::shared_ptr<CheckpointCommittableManager<CommT>>> getChkComUp(long checkpointId);

    std::shared_ptr<CommittableManager<CommT>> getEndOfInputCommittable();

    bool IsFinished() const;

    void Merge(const CommittableCollector<CommT>& cc);

    int GetNumberOfSubtasks() const;

    int GetSubtaskId() const;

    CommittableCollector<CommT> Copy() const;

    std::vector<std::shared_ptr<CheckpointCommittableManagerImpl<CommT>>> getChkCom() const;

private:
    static const long eoi;

    CheckpointCommittableMap checkpointCommittables;
    int subtaskId;
    int numberOfSubtasks;

    void AddSummary(const CommittableSummary<CommT>& summary);

    void AddCommittable(const CommittableWithLineage<CommT>& committable);

    std::shared_ptr<CheckpointCommittableManagerImpl<CommT>> GetCheckpointCommittables(
        const CommittableMessage<CommT>& committable);
};


#endif // FLINK_BENCHMARK_COMMITTABLECOLLECTOR_H
