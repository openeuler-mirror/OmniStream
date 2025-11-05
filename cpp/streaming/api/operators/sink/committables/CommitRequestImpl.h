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

#ifndef FLINK_BENCHMARK_COMMITREQUESTIMPL_H
#define FLINK_BENCHMARK_COMMITREQUESTIMPL_H

#include <memory>
#include <stdexcept>
#include <exception>
#include <iostream>
#include "Committer.h"

enum class CommitRequestState {
    RECEIVED,
    RETRY,
    FAILED,
    COMMITTED
};

inline bool IsFinalState(CommitRequestState state)
{
    switch (state) {
        case CommitRequestState::FAILED:
        case CommitRequestState::COMMITTED:
            return true;
        case CommitRequestState::RECEIVED:
        case CommitRequestState::RETRY:
            return false;
        default:
            throw std::invalid_argument("Unknown CommitRequestState");
    }
}

template <typename CommT>
class CommitRequestImpl : public CommitRequest<CommT> {
public:
    explicit CommitRequestImpl(const CommT& committable);

    CommitRequestImpl(const CommT& committable, int numRetries, const CommitRequestState& state);

    bool IsFinished() const;

    CommitRequestState GetState() const;

    CommT GetCommittable() const;

    int GetNumberOfRetries() const;

    void signalFailedWithKnownReason(const std::exception& t);

    void signalFailedWithUnknownReason(const std::exception& t);

    void RetryLater();

    void UpdateAndRetryLater(const CommT& committable);

    void SignalAlreadyCommitted();

    void SetSelected();

    void SetCommittedIfNoError();

    std::shared_ptr<CommitRequestImpl<CommT>> copy() const;

private:
    CommT committable;
    int numRetries;
    CommitRequestState state;
};

#endif // FLINK_BENCHMARK_COMMITREQUESTIMPL_H
