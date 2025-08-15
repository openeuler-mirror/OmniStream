/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// omnistream/FutureState.h
#ifndef OMNISTREAM_FUTURESTATE_H
#define OMNISTREAM_FUTURESTATE_H

#include <memory>
#include <string>

namespace omnistream {

class FutureState {
public:
    enum State {
        Completed,
      CompletedExceptional,
      Cancelled,
      Pending,
    };

    explicit FutureState(State initialState = Pending);

    void setCompleted();
    void setCompletedExceptional();
    void setCancelled();
    void setPending();

    bool isCompleted() const;
    bool isCompletedExceptional() const;
    bool isCancelled() const;
    bool isPending() const;

    std::string toString() const;

    bool operator==(const FutureState& other) const;
    bool operator!=(const FutureState& other) const;

    static std::shared_ptr<FutureState> allOf(const std::shared_ptr<FutureState>& first, const std::shared_ptr<FutureState>& second)
    {
        if (first && second && *first == *second) {
            return first;
        }

        if (!first && !second) {
            return nullptr;
        } else if (!first) {
            return second;
        } else if (!second) {
            return first;
        }

        if (first->currentState < second->currentState) {
            return first;
        } else {
            return second;
        }
    }

    static std::shared_ptr<FutureState> anyOf(const std::shared_ptr<FutureState>& first, const std::shared_ptr<FutureState>& second)
    {
        if (first && second && *first == *second) {
            return first;
        }

        if (!first && !second) {
            return nullptr;
        } else if (!first) {
            return second;
        } else if (!second) {
            return first;
        }

        if (first->currentState > second->currentState) {
            return first;
        } else {
            return second;
        }
    }

private:
    State currentState;
};

} // namespace omnistream

#endif // OMNISTREAM_FUTURESTATE_H
