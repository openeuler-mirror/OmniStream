/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by q00649235 on 2025/3/17.
//

#include "MergingWindowSet.h"

template<typename W>
MergingWindowSet<W>::MergingWindowSet(AssignerPtr windowAssigner, MapState<W, W>* mapping): mapping(mapping),
    cachedSortedWindows(MAPPING_CACHE_SIZE), sortedWindows(new std::set<W>()), windowAssigner(windowAssigner) {}

template<typename W>
void MergingWindowSet<W>::InitializeCache(BinaryRowData key)
{
    auto it = cachedSortedWindows.find(key);
    if (it == cachedSortedWindows.end()) {
        sortedWindows = new std::set<W>();
        if (mapping->entries() != nullptr) {
            for (auto &i: *mapping->entries()) {
                sortedWindows->emplace(i.first);
            }
        }
        cachedSortedWindows.emplace(key, sortedWindows);
    } else {
        sortedWindows = it->second;
    }
}

template<typename W>
W MergingWindowSet<W>::GetStateWindow(const W &window)
{
    return mapping->get(window).value();
}

template<typename W>
void MergingWindowSet<W>::RetireWindow(const W &window)
{
    mapping->remove(window);
    auto it = sortedWindows->find(window);
    if (it == sortedWindows->end()) {
        throw std::runtime_error("Window is not in in-flight window set.");
    }
    sortedWindows->erase(it);
}

template<typename W>
W MergingWindowSet<W>::AddWindow(const W &newWindow, const MergeFunction &mergeFunction)
{
    typename MergingWindowAssigner<W>::MergeResultCollector collector;
    windowAssigner->MergeWindows(newWindow, sortedWindows, collector);

    W resultWindow = newWindow;
    bool isNewWindowMerged = false;

    for (const auto &c: collector) {
        W mergeResult = c.first;
        auto mergedWindows = c.second;

        if (mergedWindows.erase(newWindow)) {
            isNewWindowMerged = true;
            resultWindow = mergeResult;
        }

        if (mergedWindows.empty()) {
            continue;
        }
        W mergedStateNamespace = mapping->get(*mergedWindows.begin()).value();

        std::vector<W> mergedStateWindows;
        for (const auto &mergedWindow: mergedWindows) {
            const std::optional<W> &optionalRes = mapping->get(mergedWindow);
            if (optionalRes.has_value()) {
                mapping->remove(mergedWindow);
                sortedWindows->erase(mergedWindow);
                if (!(optionalRes.value() == mergedStateNamespace)) {
                    mergedStateWindows.push_back(optionalRes.value());
                }
            }
        }

        mapping->put(mergeResult, mergedStateNamespace);
        sortedWindows->insert(mergeResult);

        if (!(mergedWindows.find(mergeResult) != mergedWindows.end() && mergedWindows.size() == 1)) {
            mergeFunction(mergeResult, mergedWindows, mergedStateNamespace, mergedStateWindows);
        }
    }

    // the new window created a new, self-contained window without merging
    if (collector.empty() || (resultWindow == newWindow && !isNewWindowMerged)) {
        mapping->put(resultWindow, resultWindow);
        sortedWindows->insert(resultWindow);
    }
    return resultWindow;
}
template class MergingWindowSet<TimeWindow>;
