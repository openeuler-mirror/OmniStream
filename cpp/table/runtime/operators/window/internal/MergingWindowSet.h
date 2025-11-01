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

#ifndef MERGINGWINDOWSET_H
#define MERGINGWINDOWSET_H

#pragma once

#include <map>
#include <set>
#include <vector>
#include <iostream>
#include <memory>
#include <functional>
#include "../TimeWindow.h"
#include "../assigners/MergingWindowAssigner.h"
#include "core/api/common/state/MapState.h"

template<typename W>
class MergingWindowSet {
public:
    using MergeFunction = std::function<void(W &, std::unordered_set<W, MyKeyHash> &, W &, std::vector<W> &)>;
    using AssignerPtr = std::shared_ptr<MergingWindowAssigner<W>>;
    using MappingPtr = std::unique_ptr<std::unordered_map<W, W, MyKeyHash>>;

    MergingWindowSet() = default;

    ~MergingWindowSet() = default;

    MergingWindowSet(AssignerPtr windowAssigner, MapState<W, W>* mapping);

    void InitializeCache(BinaryRowData key);

    W GetStateWindow(const W &window);

    void RetireWindow(const W &window);

    W AddWindow(const W &newWindow, const MergeFunction &mergeFunction);

    MergingWindowSet(MergingWindowSet &&) = delete;
    MergingWindowSet &operator=(MergingWindowSet &&other) noexcept
    {
        if (this != &other) {
            this->mapping = other.mapping;
            this->sortedWindows = other.sortedWindows;
            this->windowAssigner = std::move(other.windowAssigner);
        }
        return *this;
    }

private:
    static constexpr int MAPPING_CACHE_SIZE = 10000;

    // todo need to be HeapMapState, now only support same keyRowData
    MapState<W, W>* mapping;
    // need to be LRUMap
    std::unordered_map<BinaryRowData, std::set<W>*> cachedSortedWindows;
    std::set<W>* sortedWindows;
    AssignerPtr windowAssigner;
};
#endif
