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

#pragma once

#include <set>
#include <vector>
#include <functional>

#include "table/runtime/operators/window/assigners/MergingWindowAssigner.h"
#include "core/api/common/state/MapState.h"
#include "LRUMap.h"

template<typename K, typename W>
class MergingWindowSet {
public:
    using MergeFunction = std::function<void(W&, std::unordered_set<W>&, W&, std::vector<W>&)>;

    MergingWindowSet(MergingWindowAssigner<W>* windowAssigner, MapState<W, W> *mapping) : mapping(mapping), windowAssigner(windowAssigner) {}

    ~MergingWindowSet() = default;

    void InitializeCache(const K& key);

    W GetStateWindow(const W &window);

    void RetireWindow(const W &window);

    W AddWindow(const W &newWindow, const MergeFunction &mergeFunction);

private:
    static constexpr int MAPPING_CACHE_SIZE = 10000;

    MapState<W, W>* mapping;
    LRUMap<K, std::set<W>*> cachedSortedWindows{MAPPING_CACHE_SIZE};
    std::set<W>* sortedWindows = nullptr;
    MergingWindowAssigner<W>* windowAssigner;
    typename MergingWindowAssigner<W>::MergeResultCollector reuseCollector_;
};
