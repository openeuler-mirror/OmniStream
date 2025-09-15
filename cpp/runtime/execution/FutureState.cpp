/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "FutureState.h"
#include <sstream>

namespace omnistream {

    FutureState::FutureState(State initialState) : currentState(initialState) {}

    void FutureState::setCompleted() {
        currentState = Completed;
    }

    void FutureState::setCompletedExceptional() {
        currentState = CompletedExceptional;
    }

    void FutureState::setCancelled() {
        currentState = Cancelled;
    }

    void FutureState::setPending() {
        currentState = Pending;
    }

    bool FutureState::isCompleted() const {
        return currentState == Completed;
    }

    bool FutureState::isCompletedExceptional() const {
        return currentState == CompletedExceptional;
    }

    bool FutureState::isCancelled() const {
        return currentState == Cancelled;
    }

    bool FutureState::isPending() const {
        return currentState == Pending;
    }

    std::string FutureState::toString() const {
        switch (currentState) {
        case Pending:
            return "Pending";
        case Completed:
            return "Completed";
        case CompletedExceptional:
            return "CompletedExceptional";
        case Cancelled:
            return "Cancelled";
        default:
            return "Unknown";
        }
    }

    bool FutureState::operator==(const FutureState& other) const {
        return currentState == other.currentState;
    }

    bool FutureState::operator!=(const FutureState& other) const {
        return !(*this == other);
    }

} // namespace omnistream
