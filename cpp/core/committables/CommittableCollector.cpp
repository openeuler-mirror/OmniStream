/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "CommittableCollector.h"
#include <limits>
#include <sstream>

template <typename CommT>
const long CommittableCollector<CommT>::eoi = std::numeric_limits<long>::max();

template <typename CommT>
CommittableCollector<CommT>::CommittableCollector(int subtaskId, int numberOfSubtasks)
    : subtaskId(subtaskId), numberOfSubtasks(numberOfSubtasks), checkpointCommittables() {}

template <typename CommT>
CommittableCollector<CommT>::CommittableCollector(const CheckpointCommittableMap& checkpointCommittables,
                                                  int subtaskId,
                                                  int numberOfSubtasks)
    : checkpointCommittables(checkpointCommittables), subtaskId(subtaskId), numberOfSubtasks(numberOfSubtasks) {}

template <typename CommT>
CommittableCollector<CommT> CommittableCollector<CommT>::Of(int subId, int numSubtasks)
{
    return CommittableCollector<CommT>(subId, numSubtasks);
}

template <typename CommT>
CommittableCollector<CommT> CommittableCollector<CommT>::ofLegacy(const std::vector<CommT>& committables)
{
    CommittableCollector<CommT> committableCollector(0, 1);
    CommittableSummary<CommT> summary(0, 1, 0, committables.size(), committables.size(), 0);
    committableCollector.AddSummary(summary);
    for (const auto& c : committables) {
        CommittableWithLineage<CommT> committableWithLineage(c, 0, 0);
        committableCollector.AddCommittable(committableWithLineage);
    }
    return committableCollector;
}

template <typename CommT>
void CommittableCollector<CommT>::AddMessage(const CommittableMessage<CommT>& message)
{
    if (const auto* summary = dynamic_cast<const CommittableSummary<CommT>*>(&message)) {
        AddSummary(*summary);
    } else if (const auto* committable = dynamic_cast<const CommittableWithLineage<CommT>*>(&message)) {
        AddCommittable(*committable);
    } else {
        throw std::invalid_argument("Unknown message type");
    }
}

template <typename CommT>
std::vector<std::shared_ptr<CheckpointCommittableManager<CommT>>> CommittableCollector<CommT>::getChkComUp(
    long checkpointId)
{
    std::vector<std::shared_ptr<CheckpointCommittableManager<CommT>>> result;
    for (const auto& entry : checkpointCommittables) {
        if (entry.first <= checkpointId) {
            result.push_back(entry.second);
        }
    }
    return result;
}

template <typename CommT>
std::shared_ptr<CommittableManager<CommT>> CommittableCollector<CommT>::getEndOfInputCommittable()
{
    auto it = checkpointCommittables.find(eoi);
    if (it != checkpointCommittables.end()) {
        return it->second;
    }
    return nullptr;
}

template <typename CommT>
bool CommittableCollector<CommT>::IsFinished() const
{
    for (const auto& entry : checkpointCommittables) {
        if (!entry.second->IsFinished()) {
            return false;
        }
    }
    return true;
}

template <typename CommT>
void CommittableCollector<CommT>::Merge(const CommittableCollector<CommT>& cc)
{
    for (const auto& entry : cc.checkpointCommittables) {
        checkpointCommittables.merge(entry.first, entry.second);
    }
}

template <typename CommT>
int CommittableCollector<CommT>::GetNumberOfSubtasks() const
{
    return numberOfSubtasks;
}

template <typename CommT>
int CommittableCollector<CommT>::GetSubtaskId() const
{
    return subtaskId;
}

template <typename CommT>
CommittableCollector<CommT> CommittableCollector<CommT>::Copy() const
{
    CheckpointCommittableMap newCheckpointCommittables;
    for (const auto& entry : checkpointCommittables) {
        newCheckpointCommittables[entry.first] = entry.second->Copy();
    }
    return CommittableCollector<CommT>(newCheckpointCommittables, subtaskId, numberOfSubtasks);
}

template <typename CommT>
std::vector<std::shared_ptr<CheckpointCommittableManagerImpl<CommT>>> CommittableCollector<CommT>::getChkCom()
const
{
    std::vector<std::shared_ptr<CheckpointCommittableManagerImpl<CommT>>> result;
    for (const auto& entry : checkpointCommittables) {
        result.push_back(entry.second);
    }
    return result;
}

template <typename CommT>
void CommittableCollector<CommT>::AddSummary(const CommittableSummary<CommT>& summary)
{
    std::shared_ptr<CheckpointCommittableManagerImpl<CommT>> it =
            checkpointCommittables.find(summary.GetCheckpointId().value_or(eoi));
    if (it == checkpointCommittables.end()) {
        auto cv = std::make_shared<CheckpointCommittableManagerImpl<CommT>>(subtaskId,
                                                                            numberOfSubtasks,
                                                                            summary.GetCheckpointId().value_or(eoi));
        checkpointCommittables[summary.GetCheckpointId().value_or(eoi)] = cv;
        cv->UpsertSummary(summary);
    } else {
        it->UpsertSummary(summary);
    }
}

template <typename CommT>
void CommittableCollector<CommT>::AddCommittable(const CommittableWithLineage<CommT>& committable)
{
    GetCheckpointCommittables(committable)->AddCommittable(committable);
}

template <typename CommT>
std::shared_ptr<CheckpointCommittableManagerImpl<CommT>> CommittableCollector<CommT>::GetCheckpointCommittables(
    const CommittableMessage<CommT>& committable)
{
    auto it = checkpointCommittables.find(committable.GetCheckpointId().value_or(eoi));
    if (it != checkpointCommittables.end()) {
        return it->second;
    }
}