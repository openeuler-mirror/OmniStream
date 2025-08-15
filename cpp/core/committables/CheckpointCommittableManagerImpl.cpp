/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "CheckpointCommittableManagerImpl.h"
#include <limits>
#include <sstream>

template <typename CommT>
CheckpointCommittableManagerImpl<CommT>::CheckpointCommittableManagerImpl(
    int subtaskId,
    int numberOfSubtasks,
    std::optional<long> checkpointId)
    : subtaskId(subtaskId),
    numberOfSubtasks(numberOfSubtasks),
    checkpointId(checkpointId),
    subtasksCommittableManagers() {}

template <typename CommT>
CheckpointCommittableManagerImpl<CommT>::CheckpointCommittableManagerImpl(
    const SubtaskCommittableManagers& subtasksCommittableManagers,
    int subtaskId,
    int numberOfSubtasks,
    std::optional<long> checkpointId)
    : subtasksCommittableManagers(subtasksCommittableManagers),
    subtaskId(subtaskId),
    numberOfSubtasks(numberOfSubtasks),
    checkpointId(checkpointId) {}

template <typename CommT>
long CheckpointCommittableManagerImpl<CommT>::GetCheckpointId() const
{
    if (!checkpointId) {
        throw std::invalid_argument("Checkpoint ID is not set");
    }
    return *checkpointId;
}

template <typename CommT>
std::vector<std::shared_ptr<SubtaskCommittableManager<CommT>>> CheckpointCommittableManagerImpl<CommT>::getSubCM()
const
{
    std::vector<std::shared_ptr<SubtaskCommittableManager<CommT>>> managers;
    for (const auto& entry : subtasksCommittableManagers) {
        managers.push_back(entry.second);
    }
    return managers;
}

template <typename CommT>
void CheckpointCommittableManagerImpl<CommT>::UpsertSummary(const CommittableSummary<CommT>& summary)
{
    auto it = subtasksCommittableManagers.find(summary.GetSubtaskId());
    if (it == subtasksCommittableManagers.end()) {
        subtasksCommittableManagers[summary.GetSubtaskId()] = std::make_shared<SubtaskCommittableManager<CommT>>(
                summary.GetNumberOfCommittables(),
                        subtaskId,
                summary.GetCheckpointId());
    } else {
        throw std::runtime_error("Updating CommittableSummary for the same subtask is not supported");
    }
}

template <typename CommT>
void CheckpointCommittableManagerImpl<CommT>::AddCommittable(const CommittableWithLineage<CommT>& committable)
{
    getSubtaskCommittableManager(committable.GetSubtaskId())->Add(committable);
}

template <typename CommT>
std::shared_ptr<SubtaskCommittableManager<CommT>> CheckpointCommittableManagerImpl<CommT>::getSubtaskCommittableManager(
    int subId)
const
{
    auto it = subtasksCommittableManagers.find(subId);
    if (it == subtasksCommittableManagers.end()) {
        throw std::invalid_argument("Unknown subtask ID: " + std::to_string(subId));
    }
    return it->second;
}

template <typename CommT>
CommittableSummary<CommT> CheckpointCommittableManagerImpl<CommT>::GetSummary() const
{
    int totalCommittables = 0;
    int totalPending = 0;
    int totalFailed = 0;
    for (const auto& entry : subtasksCommittableManagers) {
        totalCommittables += entry.second->GetNumCommittables();
        totalPending += entry.second->GetNumPending();
        totalFailed += entry.second->GetNumFailed();
    }
    return CommittableSummary<CommT>(
            subtaskId,
            numberOfSubtasks,
            checkpointId,
            totalCommittables,
            totalPending,
            totalFailed);
}

template <typename CommT>
bool CheckpointCommittableManagerImpl<CommT>::IsFinished() const
{
    for (const auto& entry : subtasksCommittableManagers) {
        if (!entry.second->IsFinished()) {
            return false;
        }
    }
    return true;
}

template <typename CommT>
std::vector<CommittableWithLineage<CommT>> CheckpointCommittableManagerImpl<CommT>::commit(
    bool fullyReceived,
    Committer<CommT>& committer)
{
    std::vector<CommitRequestImpl<CommT>> requests = getPendingRequests(fullyReceived);
    for (auto& request : requests) {
        request.SetSelected();
    }
    committer.Commit(requests);
    for (auto& request : requests) {
        request.SetCommittedIfNoError();
    }
    return drainFinished();
}

template <typename CommT>
std::vector<CommitRequestImpl<CommT>> CheckpointCommittableManagerImpl<CommT>::getPendingRequests(bool fullyReceived)
const
{
    std::vector<CommitRequestImpl<CommT>> requests;
    for (const auto& entry : subtasksCommittableManagers) {
        if (!fullyReceived || entry.second->HasReceivedAll()) {
            for (const auto& request : entry.second->GetPendingRequests()) {
                requests.push_back(request);
            }
        }
    }
    return requests;
}

template <typename CommT>
std::vector<CommittableWithLineage<CommT>> CheckpointCommittableManagerImpl<CommT>::drainFinished()
{
    std::vector<CommittableWithLineage<CommT>> finished;
    for (const auto& entry : subtasksCommittableManagers) {
        for (const auto& committable : entry.second->DrainCommitted()) {
            finished.push_back(committable);
        }
    }
    return finished;
}

template <typename CommT>
CheckpointCommittableManagerImpl<CommT> CheckpointCommittableManagerImpl<CommT>::Merge(
    const CheckpointCommittableManagerImpl<CommT>& other)
{
    if (checkpointId != other.checkpointId) {
        throw std::invalid_argument("Checkpoint IDs do not match");
    }
    for (const auto& entry : other.subtasksCommittableManagers) {
        subtasksCommittableManagers.merge(
            entry.first,
            entry.second,
            [] (const std::shared_ptr<SubtaskCommittableManager<CommT>>& a,
                const std::shared_ptr<SubtaskCommittableManager<CommT>>& b) {
            return a->Merge(*b);
        });
    }
    return *this;
}