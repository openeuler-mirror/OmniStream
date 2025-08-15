/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/26/25.
//

#ifndef OMNISTREAM_EXECUTIONSTATE_H
#define OMNISTREAM_EXECUTIONSTATE_H

#include <string>
#include <memory>
#include <sstream>

namespace omnistream
{
    enum class ExecutionState
    {
        CREATED,
        SCHEDULED,
        DEPLOYING,
        RUNNING,
        FINISHED,
        CANCELING,
        CANCELED,
        FAILED,
        RECONCILING,
        INITIALIZING
    };

    class ExecutionStateWrapper
    {
    public:
  explicit  ExecutionStateWrapper(ExecutionState state = ExecutionState::CREATED) :
        state_(state) {};
        ~ExecutionStateWrapper();

        ExecutionState getState() const { return state_; };
        void setState(ExecutionState state) { state_ = state; };

        bool isTerminal() const
        {
            return state_ == ExecutionState::FINISHED || state_ == ExecutionState::CANCELED || state_ ==
                ExecutionState::FAILED;
        };

        std::string toString() const
        {
            switch (state_)
            {
            case ExecutionState::CREATED:
                return "CREATED";
            case ExecutionState::SCHEDULED:
                return "SCHEDULED";
            case ExecutionState::DEPLOYING:
                return "DEPLOYING";
            case ExecutionState::RUNNING:
                return "RUNNING";
            case ExecutionState::FINISHED:
                return "FINISHED";
            case ExecutionState::CANCELING:
                return "CANCELING";
            case ExecutionState::CANCELED:
                return "CANCELED";
            case ExecutionState::FAILED:
                return "FAILED";
            case ExecutionState::RECONCILING:
                return "RECONCILING";
            case ExecutionState::INITIALIZING:
                return "INITIALIZING";
            default:
                return "UNKNOWN";
            }
        };

    private:
        ExecutionState state_;
    };

} // namespace omnistream

#endif // OMNISTREAM_EXECUTIONSTATE_H
