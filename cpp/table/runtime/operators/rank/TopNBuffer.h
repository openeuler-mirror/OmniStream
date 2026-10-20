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

#ifndef OMNISTREAM_TOPNBUFFER_H
#define OMNISTREAM_TOPNBUFFER_H

#include <map> // map is sorted, the equivalence of java's TreeMap
#include "table/data/RowData.h"
#include "table/runtime/keyselector/KeySelector.h"

using Comparator = bool (*)(RowData*, RowData*);

struct DescendingComparator {
    bool operator()(RowData* a, RowData* b) const
    {
        return *(a->getLong(0)) > *(b->getLong(0)); // Sort in descending order
    }
};
struct AscendingComparator {
    bool operator()(RowData* a, RowData* b) const
    {
        return *(a->getLong(0)) < *(b->getLong(0)); // Sort in ascending order
    }
};
class TopNBuffer {
public:
    // sortkey always uses binaryRowData
    TopNBuffer() = default;
    auto inline begin()
    {
        return treeMap.begin();
    }
    auto inline end()
    {
        return treeMap.end();
    }
    int put(RowData* sortKey, RowData* value)
    {
        currentTopNum += 1;
        auto [it, inserted] = treeMap.try_emplace(sortKey, nullptr);
        if (inserted) {
            auto newList = new std::vector<RowData*>(1, value);
            it->second = newList;
            return 1;
        } else {
            it->second->push_back(value);
            return it->second->size();
        }
    }

    auto get(RowData* sortKey)
    {
        return treeMap[sortKey];
    }

    bool contains(RowData* sortKey)
    {
        return treeMap.find(sortKey) != treeMap.end();
    }

    // Detach the sort key. Caller owns the returned list and frees its RowData.
    std::vector<RowData*>* removeAll(RowData* sortKey)
    {
        auto it = treeMap.find(sortKey);
        if (it == treeMap.end()) {
            return nullptr;
        }
        auto* list = it->second;
        if (list != nullptr) {
            currentTopNum -= static_cast<int>(list->size());
        }
        treeMap.erase(it);
        return list;
    }

    // Pop the worst row. When that empties the sort-key list, *droppedKey is the
    // sort key and the empty list is freed here. Caller owns the returned row.
    RowData* removeLast(RowData** droppedKey)
    {
        if (droppedKey != nullptr) {
            *droppedKey = nullptr;
        }
        if (treeMap.empty()) {
            return nullptr;
        }

        auto last = --treeMap.end();
        auto* collection = last->second;
        if (collection == nullptr || collection->empty()) {
            if (droppedKey != nullptr) {
                *droppedKey = last->first;
            }
            delete collection;
            treeMap.erase(last);
            return nullptr;
        }

        RowData* lastElement = collection->back();
        collection->pop_back();
        currentTopNum--;
        if (collection->empty()) {
            if (droppedKey != nullptr) {
                *droppedKey = last->first;
            }
            delete collection;
            treeMap.erase(last);
        }
        return lastElement;
    }

    void removeLast(std::vector<RowData*>* collection, RowData* lastElement, RowData* element)
    {
        if (lastElement == nullptr) {
            return;
        }

        for (auto it = collection->begin(); it != collection->end(); ++it) {
            if (*it == lastElement) {
                collection->erase(it);
                currentTopNum--;
                break;
            }
        }
        if (collection->empty()) {
            treeMap.erase(element);
        }
    }

    void putAll(RowData* sortKey, std::vector<RowData*>* values)
    {
        auto it = treeMap.find(sortKey);
        if (it != treeMap.end()) {
            currentTopNum -= static_cast<int>(it->second->size());
            it->second = values;
        } else {
            treeMap[sortKey] = values; // Insert new entry in the map
        }
        currentTopNum += static_cast<int>(values->size());
    }
    auto lastEntry()
    {
        return treeMap.rbegin();
    }

    int getCurrentTopNum()
    {
        return currentTopNum;
    }

    RowData* lastElement()
    {
        if (treeMap.empty()) {
            return nullptr;
        }

        auto lastEntry = --treeMap.end();
        std::vector<RowData*>* collection = lastEntry->second;

        if (collection != nullptr) {
            return getLastElement(*collection);
        }

        return nullptr;
    }

    RowData* getLastElement(const std::vector<RowData*>& collection)
    {
        if (!collection.empty()) {
            return collection.back();
        }
        return nullptr;
    }

    // This method checks if the given sortKey is in the buffer range according to the specified topNum.
    bool checkSortKeyInBufferRange(RowData* sortKey, long topNum)
    {
        auto worstEntry = lastEntry();
        // If this key doesn't exist
        if (worstEntry == treeMap.rend()) {
            // return true if the buffer is empty.
            return true;
        } else {
            RowData* worstKey = worstEntry->first;
            bool compare = DescendingComparator{}(sortKey, worstKey);
            return compare || (currentTopNum < topNum);
        }
    }

private:
    int currentTopNum = 0;
    std::map<RowData*, std::vector<RowData*>*, DescendingComparator> treeMap;
};

#endif
