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

#include "HeapPriorityQueueSet.h"
#include "state/PriorityQueueSetFactory.h"

class HeapPriorityQueueSetFactory : public PriorityQueueSetFactory {
public:
    HeapPriorityQueueSetFactory(
            KeyGroupRange* keyGroupRange,
            int32_t totalKeyGroups,
            int32_t minimumCapacity)
            :
            keyGroupRange_(keyGroupRange),
            totalKeyGroups_(totalKeyGroups),
            minimumCapacity_(minimumCapacity) {}

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> create(
            std::string stateName,
            TypeSerializer* byteOrderedElementSerializer) {
        return std::make_shared<HeapPriorityQueueSet<K, T, Comparator>>(keyGroupRange_, minimumCapacity_, totalKeyGroups_);
    }

private:
    KeyGroupRange* keyGroupRange_;
    int32_t totalKeyGroups_;
    int32_t minimumCapacity_;
};
