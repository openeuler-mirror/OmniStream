/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "CommitRequestImpl.h"
#include <sstream>

template <typename CommT>
CommitRequestImpl<CommT>::CommitRequestImpl(const CommT& committable)
    : committable(committable), numRetries(0), state(CommitRequestState::RECEIVED) {}

template <typename CommT>
CommitRequestImpl<CommT>::CommitRequestImpl(const CommT& committable, int numRetries, const CommitRequestState& state)
    : committable(committable), numRetries(numRetries), state(state) {}

template <typename CommT>
bool CommitRequestImpl<CommT>::IsFinished() const
{
    return IsFinalState(state);
}

template <typename CommT>
CommitRequestState CommitRequestImpl<CommT>::GetState() const
{
    return state;
}

template <typename CommT>
CommT CommitRequestImpl<CommT>::GetCommittable() const
{
    return committable;
}

template <typename CommT>
int CommitRequestImpl<CommT>::GetNumberOfRetries() const
{
    return numRetries;
}

template <typename CommT>
void CommitRequestImpl<CommT>::signalFailedWithKnownReason(const std::exception& t)
{
    state = CommitRequestState::FAILED;
}

template <typename CommT>
void CommitRequestImpl<CommT>::signalFailedWithUnknownReason(const std::exception& t)
{
    state = CommitRequestState::FAILED;
    throw std::runtime_error("FAILED to Commit " + std::string(committable));
}

template <typename CommT>
void CommitRequestImpl<CommT>::RetryLater()
{
    state = CommitRequestState::RETRY;
    numRetries++;
}

template <typename CommT>
void CommitRequestImpl<CommT>::UpdateAndRetryLater(const CommT& com)
{
    this->committable = com;
    RetryLater();
}

template <typename CommT>
void CommitRequestImpl<CommT>::SignalAlreadyCommitted()
{
    state = CommitRequestState::COMMITTED;
}

template <typename CommT>
void CommitRequestImpl<CommT>::SetSelected()
{
    state = CommitRequestState::RECEIVED;
}

template <typename CommT>
void CommitRequestImpl<CommT>::SetCommittedIfNoError()
{
    if (state == CommitRequestState::RECEIVED) {
        state = CommitRequestState::COMMITTED;
    }
}

template <typename CommT>
std::shared_ptr<CommitRequestImpl<CommT>> CommitRequestImpl<CommT>::copy() const
{
    return std::make_shared<CommitRequestImpl<CommT>>(committable, numRetries, state);
}