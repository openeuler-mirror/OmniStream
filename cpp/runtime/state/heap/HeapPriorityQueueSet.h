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
#ifndef FLINK_TNEL_HEAPPRIORITYQUEUESET_H
#define FLINK_TNEL_HEAPPRIORITYQUEUESET_H
#include <climits>
#include <vector>
#include <boost/heap/d_ary_heap.hpp>
#include "runtime/state/KeyGroupRange.h"
#include "emhash7.hpp"
#include "runtime/state/KeyGroupRangeAssignment.h"

template <typename T, typename Comparator>
using HeapPriorityQueue = boost::heap::d_ary_heap<T, boost::heap::mutable_<true>, boost::heap::arity<3>, boost::heap::compare<Comparator>>;

// the iterator in boost::heap::d_ary_heap cannot be directly stored in hash map, so we need to wrap it
template <typename T, typename Comparator>
struct IteratorWrapper {
    typename HeapPriorityQueue<T, Comparator>::handle_type iter;
};

template <typename T, typename Comparator>
class HeapPriorityQueueSet {
public:
    HeapPriorityQueueSet(KeyGroupRange *keyGroupRange, int minCapacity, int totalNumberOfKeyGroups);

    ~HeapPriorityQueueSet();

    bool isEmpty();

    template <typename K>
    T poll();

    template <typename K>
    bool add(T element);

    template <typename K>
    bool remove(T toRemove);

    T peek();

    void clear();
private:
    // Notice:
    // In Flink's HeapPriorityQueueSet, among elements with the same priority, the one inserted first will be placed earlier
    // However, the relative order of elements with the same priority in boost::heap::d_ary_heap is unspecified.
    // Currently, this difference only affects the trigger order of timers with the same timestamp, and no other impacts have been observed.
    HeapPriorityQueue<T, Comparator> priorityQueue;

    int totalNumberOfKeyGroups;
    KeyGroupRange *keyGroupRange;
    std::vector<emhash7::HashMap<T, IteratorWrapper<T, Comparator>> *> deduplicationMapsByKeyGroup;

    int globalKeyGroupToLocalIndex(int keyGroup);
    template <typename K>
    emhash7::HashMap<T, IteratorWrapper<T, Comparator>> *getDedupMapForElement(T element);
    emhash7::HashMap<T, IteratorWrapper<T, Comparator>> *getDedupMapForKeyGroup(int keyGroupId);
};

template <typename T, typename Comparator>
int HeapPriorityQueueSet<T, Comparator>::globalKeyGroupToLocalIndex(int keyGroup)
{
    if (!keyGroupRange->contains(keyGroup)) {
        THROW_LOGIC_EXCEPTION("Key group " + std::to_string(keyGroup) + " is not included in" +
                              "[" + std::to_string(keyGroupRange->getStartKeyGroup()) + ", " + std::to_string(keyGroupRange->getEndKeyGroup()) + "]")
    }
    return keyGroup - keyGroupRange->getStartKeyGroup();
}

template <typename T, typename Comparator>
template <typename K>
inline emhash7::HashMap<T, IteratorWrapper<T, Comparator>> *HeapPriorityQueueSet<T, Comparator>::getDedupMapForElement(T element)
{
    K key = element->getKey();
    int keyGroup = KeyGroupRangeAssignment<K>::assignToKeyGroup(key, totalNumberOfKeyGroups);
    return getDedupMapForKeyGroup(keyGroup);
}

template <typename T, typename Comparator>
emhash7::HashMap<T, IteratorWrapper<T, Comparator>> *HeapPriorityQueueSet<T, Comparator>::getDedupMapForKeyGroup(int keyGroupId)
{
    return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupId)];
}

template <typename T, typename Comparator>
HeapPriorityQueueSet<T, Comparator>::HeapPriorityQueueSet(KeyGroupRange *keyGroupRange, int minCapacity, int totalNumberOfKeyGroups) : totalNumberOfKeyGroups(totalNumberOfKeyGroups), keyGroupRange(keyGroupRange)
{
    int keyGroupsInLocalRange = keyGroupRange->getNumberOfKeyGroups();
    if (keyGroupsInLocalRange <= 0) {
        THROW_LOGIC_EXCEPTION("keyGroupsInLocalRange should be larger than 0.")
    }
    int deduplicationSetSize = 1 + minCapacity / keyGroupsInLocalRange;
    for (int i = 0; i < keyGroupsInLocalRange; ++i) {
        auto tempMap = new emhash7::HashMap<T, IteratorWrapper<T, Comparator>>();
        tempMap->reserve(deduplicationSetSize);
        deduplicationMapsByKeyGroup.push_back(tempMap);
    }
}

template <typename T, typename Comparator>
HeapPriorityQueueSet<T, Comparator>::~HeapPriorityQueueSet()
{
    clear();
    for (size_t i = 0; i < deduplicationMapsByKeyGroup.size(); ++i) {
        delete deduplicationMapsByKeyGroup[i];
    }
}

template <typename T, typename Comparator>
bool HeapPriorityQueueSet<T, Comparator>::isEmpty()
{
    return priorityQueue.empty();
}

template <typename T, typename Comparator>
template <typename K>
T HeapPriorityQueueSet<T, Comparator>::poll()
{
    if (priorityQueue.empty()) {
        return nullptr;
    }

    T toRemove = priorityQueue.top();

    auto *map = getDedupMapForElement<K>(toRemove);
    auto it = map->find(toRemove);
    if (it == map->end()) {
        THROW_LOGIC_EXCEPTION("Element exists in priority queue but not in dedup map (data inconsistency)")
    }
    map->erase(it);
    priorityQueue.pop();
    return toRemove;
}

template <typename T, typename Comparator>
template <typename K>
bool HeapPriorityQueueSet<T, Comparator>::add(T element)
{
    auto *map = getDedupMapForElement<K>(element);
    auto mapIter = map->find(element);
    bool inMap = mapIter != map->end();
    bool changed = false;
    if (!inMap) {
        T oldHead;
        if (priorityQueue.empty()) {
            changed = true;
        } else {
            oldHead = priorityQueue.top();
        }

        auto queueIter = priorityQueue.push(element);
        map->emplace(element, IteratorWrapper<T, Comparator>{queueIter});
        changed = changed ? true : oldHead != priorityQueue.top();
    } else {
        delete element;
    }
    return changed;
}

template <typename T, typename Comparator>
template <typename K>
bool HeapPriorityQueueSet<T, Comparator>::remove(T toRemove)
{
    auto *map = getDedupMapForElement<K>(toRemove);
    auto mapIter = map->find(toRemove);
    bool inMap = mapIter != map->end();
    if (inMap) {
        auto oldHead = priorityQueue.top();
        auto queueIter = mapIter->second.iter;
        map->erase(mapIter);
        priorityQueue.erase(queueIter);
        return oldHead != priorityQueue.top();
    }
    return false;
}

template <typename T, typename Comparator>
T HeapPriorityQueueSet<T, Comparator>::peek()
{
    return priorityQueue.empty() ? nullptr : priorityQueue.top();
}

template <typename T, typename Comparator>
void HeapPriorityQueueSet<T, Comparator>::clear()
{
    while (!priorityQueue.empty()) {
        delete priorityQueue.top();
        priorityQueue.pop();
    }
    for (auto *map: deduplicationMapsByKeyGroup) {
        map->clear();
    }
}

#endif // FLINK_TNEL_HEAPPRIORITYQUEUESET_H