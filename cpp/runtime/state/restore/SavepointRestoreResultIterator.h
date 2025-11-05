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

#ifndef OMNISTREAM_SAVEPOINTRESTORERESULTITERATOR_H
#define OMNISTREAM_SAVEPOINTRESTORERESULTITERATOR_H

#include "SavepointRestoreResult.h"
#include "KeyGroup.h"
#include "KeyGroupIterator.h"
#include "runtime/state/KeyedStateHandle.h"

class SavepointRestoreResultIterator {
public:
    SavepointRestoreResultIterator()
        : currentIndex_(0) {
    }

    SavepointRestoreResultIterator(
            const std::vector<std::shared_ptr<KeyedStateHandle>>& stateHandles)
        : stateHandles_(stateHandles), currentIndex_(0) {
    }

    bool hasNext()
    {
        return currentIndex_ < stateHandles_.size();
    }

    std::unique_ptr<SavepointRestoreResult> next()
    {
        if (!hasNext()) {
            throw std::out_of_range("No more elements in SavepointRestoreResultIterator");
        }

        // In a real implementation, this would deserialize the state handle
        // and extract the actual state metadata and key groups
        // For now, we create a placeholder

        std::vector<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
        // This would involve deserializing the savepoint data

        std::vector<KeyGroup> keyGroups;
        // This would involve reading the key-value pairs from the savepoint

        auto keyGroupIterator = std::make_unique<KeyGroupIterator>(keyGroups);

        return std::make_unique<SavepointRestoreResult>(
                stateMetaInfoSnapshots,
                std::move(keyGroupIterator)
        );
    };

private:
    std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles_;
    size_t currentIndex_;
};

#endif // OMNISTREAM_SAVEPOINTRESTORERESULTITERATOR_H
