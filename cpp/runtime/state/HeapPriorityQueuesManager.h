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
#include <memory>
#include <string>
#include <unordered_map>

#include "KeyGroupRange.h"
#include "heap/HeapPriorityQueueSetFactory.h"
#include "heap/HeapPriorityQueueSnapshotRestoreWrapper.h"

class HeapPriorityQueuesManager {
public:
    HeapPriorityQueuesManager(
            std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>> registeredPQStates,
            std::shared_ptr<HeapPriorityQueueSetFactory> priorityQueueSetFactory,
            KeyGroupRange* keyGroupRange,
            int32_t numberOfKeyGroups)
            :
            registeredPQStates_(registeredPQStates),
            priorityQueueSetFactory_(priorityQueueSetFactory),
            keyGroupRange_(keyGroupRange),
            numberOfKeyGroups_(numberOfKeyGroups) {}

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> createOrUpdate(
            std::string stateName,
            TypeSerializer* byteOrderedElementSerializer) {
        return createOrUpdate<K, T, Comparator>(stateName, byteOrderedElementSerializer, false);
    }

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> createOrUpdate(
            std::string stateName,
            TypeSerializer* byteOrderedElementSerializer,
            bool allowFutureMetadataUpdates) {
        auto iter = registeredPQStates_->find(stateName);
        if (iter != registeredPQStates_->end()) {
            auto existingState = std::dynamic_pointer_cast<HeapPriorityQueueSnapshotRestoreWrapper<K, T, Comparator>>(iter->second);
            if (existingState == nullptr) {
                THROW_LOGIC_EXCEPTION("Priority queue type is not HeapPriorityQueueSnapshotRestoreWrapper")
            }
            // todo: TypeSerializerSchemaCompatibility
            return existingState->getHeapPriorityQueueSet();
        }
        auto metaInfo = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(stateName, byteOrderedElementSerializer);

        // todo: withSerializerUpgradesAllowed
        return createInternal<K, T, Comparator>(metaInfo);
    }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>> getRegisteredPQStates() {
        return registeredPQStates_;
    }

private:
    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> createInternal(
            std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo) {
        const std::string& stateName = metaInfo->getName();

        std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> priorityQueue =
            priorityQueueSetFactory_->create<K, T, Comparator>(stateName, metaInfo->getElementSerializer());

        using WrapperType = HeapPriorityQueueSnapshotRestoreWrapper<K, T, Comparator>;
        std::shared_ptr<WrapperType> wrapper = std::make_shared<WrapperType>(
            std::static_pointer_cast<HeapPriorityQueueSet<K, T, Comparator>>(priorityQueue),
            metaInfo,
            keyGroupRange_,
            numberOfKeyGroups_
        );

        registeredPQStates_->emplace(stateName, wrapper);

        return priorityQueue;
    }

    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapperBase>>> registeredPQStates_;
    std::shared_ptr<HeapPriorityQueueSetFactory> priorityQueueSetFactory_;
    KeyGroupRange* keyGroupRange_;
    int32_t numberOfKeyGroups_;
};




