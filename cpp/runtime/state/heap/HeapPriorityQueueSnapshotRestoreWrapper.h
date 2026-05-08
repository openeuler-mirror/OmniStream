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
#include "HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "state/RegisteredPriorityQueueStateBackendMetaInfo.h"

template <typename K, typename T, typename Comparator>
class HeapPriorityQueueSnapshotRestoreWrapper : public HeapPriorityQueueSnapshotRestoreWrapperBase {
public:
    HeapPriorityQueueSnapshotRestoreWrapper(
            std::shared_ptr<HeapPriorityQueueSet<K, T, Comparator>> heapPriorityQueueSet,
            std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo,
            KeyGroupRange* keyGroupRange,
            int32_t totalKeyGroupSize)
            : heapPriorityQueueSet_(heapPriorityQueueSet),
            metaInfo_(metaInfo),
            keyGroupRange_(keyGroupRange),
            totalKeyGroupSize_(totalKeyGroupSize) {}

    std::shared_ptr<HeapPriorityQueueSet<K, T, Comparator>> getHeapPriorityQueueSet() {
        return heapPriorityQueueSet_;
    }

    std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> getMetaInfo() {
        return metaInfo_;
    }

private:
    std::shared_ptr<HeapPriorityQueueSet<K, T, Comparator>> heapPriorityQueueSet_;
    std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo_;
    KeyGroupRange* keyGroupRange_;
    int32_t totalKeyGroupSize_;
};



