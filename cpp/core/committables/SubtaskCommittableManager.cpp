/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "SubtaskCommittableManager.h"
#include <sstream>

template <typename CommT>
SubtaskCommittableManager<CommT>::SubtaskCommittableManager(
    int numExpectedCommittables,
    int subtaskId,
    std::optional<long> checkpointId)
    : numExpectedCommittables(numExpectedCommittables),
    checkpointId(checkpointId),
    subtaskId(subtaskId),
    numDrained(0),
    numFailed(0),
    requests() {}

template <typename CommT>
SubtaskCommittableManager<CommT>::SubtaskCommittableManager(
    const std::vector<std::shared_ptr<CommitRequestImpl<CommT>>>& requests,
    int numExpectedCommittables,
    int numDrained,
    int numFailed,
    int subtaskId,
    std::optional<long> checkpointId)
    : numExpectedCommittables(numExpectedCommittables),
    checkpointId(checkpointId),
    subtaskId(subtaskId),
    numDrained(numDrained),
    numFailed(numFailed),
    requests(requests.begin(), requests.end()) {}

template <typename CommT>
void SubtaskCommittableManager<CommT>::Add(const CommittableWithLineage<CommT>& committable)
{
    Add(committable.GetCommittable());
}

template <typename CommT>
void SubtaskCommittableManager<CommT>::Add(const CommT& committable)
{
    if (requests.size() >= numExpectedCommittables) {
        throw std::runtime_error("Already received all committables.");
    }
    requests.push_back(std::make_shared<CommitRequestImpl<CommT>>(committable));
}

template <typename CommT>
bool SubtaskCommittableManager<CommT>::HasReceivedAll() const
{
    return GetNumCommittables() == numExpectedCommittables;
}

template <typename CommT>
int SubtaskCommittableManager<CommT>::GetNumCommittables() const
{
    return requests.size() + numDrained + numFailed;
}

template <typename CommT>
int SubtaskCommittableManager<CommT>::GetNumPending() const
{
    return numExpectedCommittables - (numDrained + numFailed);
}

template <typename CommT>
int SubtaskCommittableManager<CommT>::GetNumFailed() const
{
    return numFailed;
}

template <typename CommT>
bool SubtaskCommittableManager<CommT>::IsFinished() const
{
    return GetNumPending() == 0;
}

template <typename CommT>
std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> SubtaskCommittableManager<CommT>::GetPendingRequests() const
{
    std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> pendingRequests;
    for (const auto& request : requests) {
        if (!request->IsFinished()) {
            pendingRequests.push_back(request);
        }
    }
    return pendingRequests;
}

template <typename CommT>
std::vector<CommittableWithLineage<CommT>> SubtaskCommittableManager<CommT>::DrainCommitted()
{
    std::vector<CommittableWithLineage<CommT>> committed;
    auto it = requests.begin();
    while (it != requests.end()) {
        if ((*it)->IsFinished()) {
            if ((*it)->GetState() == CommitRequestImpl<CommT>::FAILED) {
                numFailed += 1;
                it = requests.erase(it);
                continue;
            } else {
                committed.emplace_back((*it)->GetCommittable(), checkpointId, subtaskId);
            }
        }
        ++it;
    }
    numDrained += committed.size();
    return committed;
}

template <typename CommT>
int SubtaskCommittableManager<CommT>::GetNumDrained() const
{
    return numDrained;
}

template <typename CommT>
int SubtaskCommittableManager<CommT>::GetSubtaskId() const
{
    return subtaskId;
}

template <typename CommT>
std::optional<long> SubtaskCommittableManager<CommT>::GetCheckpointId() const
{
    return checkpointId;
}

template <typename CommT>
std::deque<std::shared_ptr<CommitRequestImpl<CommT>>> SubtaskCommittableManager<CommT>::GetRequests() const
{
    return requests;
}

template <typename CommT>
SubtaskCommittableManager<CommT> SubtaskCommittableManager<CommT>::Merge(const SubtaskCommittableManager<CommT>& other)
{
    if (other.GetSubtaskId() != this->GetSubtaskId()) {
        throw std::invalid_argument("Subtask IDs do not match");
    }
    this->numExpectedCommittables += other.numExpectedCommittables;
    this->requests.insert(this->requests.end(), other.requests.begin(), other.requests.end());
    this->numDrained += other.numDrained;
    this->numFailed += other.numFailed;
    return *this;
}

template <typename CommT>
SubtaskCommittableManager<CommT> SubtaskCommittableManager<CommT>::Copy() const
{
    std::vector<std::shared_ptr<CommitRequestImpl<CommT>>> copiedRequests;
    for (const auto& request : requests) {
        copiedRequests.push_back(request->Copy());
    }
    return SubtaskCommittableManager<CommT>(
            copiedRequests,
            numExpectedCommittables,
            numDrained,
            numFailed,
            subtaskId,
            checkpointId);
}