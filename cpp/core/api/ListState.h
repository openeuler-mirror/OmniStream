/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_LISTSTATE_H
#define FLINK_TNEL_LISTSTATE_H

#include <vector>
#include <optional>
#include "core/api/common/state/State.h"
template<typename UV>
class ListState : virtual public State {
public:
    virtual ~ListState() = default;

    // Adds a single value to the list state
    virtual void add(const UV &value) = 0;

    // Updates the list state with a new set of values
    virtual void update(const std::vector<UV> &values) = 0;

    // Retrieves the current list state
    virtual std::vector<UV>* get() = 0;

    // Merges another list into the current state
    virtual void merge(const std::vector<UV> &other) = 0;

    // Adds another list into the current list
    virtual void addAll(const std::vector<UV> &values) = 0;

    // Clears the list state
    virtual void clear() = 0;
};

#endif //FLINK_TNEL_LISTSTATE_H
