/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by q00649235 on 2025/3/17.
//

#ifndef MERGINGWINDOWASSIGNER_H
#define MERGINGWINDOWASSIGNER_H

#pragma once

#include <unordered_set>
#include <set>
#include "WindowAssigner.h"
#include "table/runtime/operators/window/TimeWindow.h"

template<typename W>
class MergingWindowAssigner : public WindowAssigner<W> {
public:
    using MergeResultCollector = std::unordered_map<W, std::unordered_set<W, MyKeyHash>, MyKeyHash>;

    virtual void MergeWindows(const W &newWindow, std::set<W> *sortedWindows, MergeResultCollector &callback) = 0;
};

#endif
