/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_HEAPPRIORITYQUEUESET_H
#define FLINK_TNEL_HEAPPRIORITYQUEUESET_H
#include <climits>
#include <queue>
#include <vector>
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "table/runtime/operators/TimerHeapInternalTimer.h"
#include "emhash7.hpp"
#include "runtime/state/KeyGroupRangeAssignment.h"

template <typename T, typename Comparator>
class HeapPriorityQueueSet
{
public:
    HeapPriorityQueueSet(KeyGroupRange *keyGroupRange, int minCapacity, int totalNumberOfKeyGroups);
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
    std::priority_queue<T, std::vector<T>, Comparator>
        priorityQueue;

    int totalNumberOfKeyGroups = 128;
    KeyGroupRange *keyGroupRange;
    std::vector<emhash7::HashMap<T, T, std::hash<T>, std::equal_to<T>> *> deduplicationMapsByKeyGroup;

    int globalKeyGroupToLocalIndex(int keyGroup);
    template <typename K>
    emhash7::HashMap<T, T, std::hash<T>, std::equal_to<T>> *getDedupMapForElement(T element);
    emhash7::HashMap<T, T, std::hash<T>, std::equal_to<T>> *getDedupMapForKeyGroup(int keyGroupId);
    bool removeFromQueue(T element);
};

template <typename T, typename Comparator>
int HeapPriorityQueueSet<T, Comparator>::globalKeyGroupToLocalIndex(int keyGroup)
{
    int startKeyGroup = keyGroupRange->getStartKeyGroup();
    if (keyGroup < startKeyGroup)
    {
        THROW_LOGIC_EXCEPTION("Does not include key group")
    }
    return keyGroup - startKeyGroup;
}

template <typename T, typename Comparator>
template <typename K>
inline emhash7::HashMap<T, T, std::hash<T>, std::equal_to<T>> *HeapPriorityQueueSet<T, Comparator>::getDedupMapForElement(T element)
{
    std::hash<K> keyHash;
    K key = element->getKey();
    // should be keyHash(key) % totalNumberOfKeyGroups?
    int keyGroup = keyHash(key) % 128;
    return getDedupMapForKeyGroup(keyGroup);
}

template <typename T, typename Comparator>
emhash7::HashMap<T, T, std::hash<T>, std::equal_to<T>> *HeapPriorityQueueSet<T, Comparator>::getDedupMapForKeyGroup(int keyGroupId)
{
    return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupId)];
}

template <typename T, typename Comparator>
bool HeapPriorityQueueSet<T, Comparator>::removeFromQueue(T element)
{
    std::vector<T> temp;
    bool removed = false;

    while (!priorityQueue.empty())
    {
        if (*(priorityQueue.top()) != *element)
        {
            temp.push_back(priorityQueue.top());
            priorityQueue.pop();
        }
        else
        {
            priorityQueue.pop();
            removed = true;
            break;
        }
    }

    for (const auto &item : temp)
    {
        priorityQueue.push(item);
    }

    return removed;
}

template <typename T, typename Comparator>
HeapPriorityQueueSet<T, Comparator>::HeapPriorityQueueSet(KeyGroupRange *keyGroupRange, int minCapacity, int totalNumberOfKeyGroups) : totalNumberOfKeyGroups(totalNumberOfKeyGroups), keyGroupRange(keyGroupRange)
{
    int keyGroupsInLocalRange = keyGroupRange->getNumberOfKeyGroups();
    deduplicationMapsByKeyGroup.resize(keyGroupsInLocalRange, new emhash7::HashMap<T, T, std::hash<T>, std::equal_to<T>>());
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
    if (priorityQueue.empty())
    {
        return nullptr;
    }

    T toRemove = priorityQueue.top();
    priorityQueue.pop();

    auto map = getDedupMapForElement<K>(toRemove);
    auto it = map->find(toRemove);
    if (it != map->end())
    {
        map->erase(it);
    }
    return toRemove;
}

template <typename T, typename Comparator>
template <typename K>
bool HeapPriorityQueueSet<T, Comparator>::add(T element)
{
    auto *map = getDedupMapForElement<K>(element);
    auto it = map->find(element);
    bool inMap = it != map->end();
    if (!inMap)
    {
        bool change = false;
        T oldTop;
        if (priorityQueue.size() == 0)
        {
            change = true;
        }
        else
        {
            oldTop = priorityQueue.top();
        }
        map->emplace(std::make_pair(element, element));
        priorityQueue.push(element);
        return change ? true : oldTop != priorityQueue.top();
    }
    return false;
}

template <typename T, typename Comparator>
template <typename K>
bool HeapPriorityQueueSet<T, Comparator>::remove(T toRemove)
{
    auto *map = getDedupMapForElement<K>(toRemove);
    auto it = map->find(toRemove);
    bool inMap = it != map->end();
    if (inMap)
    {
        map->erase(it);
        return removeFromQueue(toRemove);
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
    while (!priorityQueue.empty())
    {
        priorityQueue.pop();
    }
    for (auto *map : deduplicationMapsByKeyGroup)
    {
        map->clear();
    }
}

#endif // FLINK_TNEL_HEAPPRIORITYQUEUESET_H
