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

#include "core/utils/Iterator.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/checkpoint/StateObjectCollection.h"

using namespace omnistream::utils;

/**
 * Iterator for iterating over prioritized raw operator state collections.
 */
class OperatorStateHandleIterator : public omnistream::utils::Iterator<std::shared_ptr<StateObjectCollection<OperatorStateHandle>>> {
public:
    OperatorStateHandleIterator(const std::vector<StateObjectCollection<OperatorStateHandle>>& prioritizedStates)
        : prioritizedStates_(prioritizedStates),
          currentIndex_(0) {}
    
    ~OperatorStateHandleIterator() override = default;
    
    bool hasNext() override {
        return currentIndex_ < prioritizedStates_.size();
    }
    
    std::shared_ptr<StateObjectCollection<OperatorStateHandle>> next() override {
        if (!hasNext()) {
            return nullptr;
        }
        
        // Create a new shared_ptr to avoid ownership issues
        auto collection = std::make_shared<StateObjectCollection<OperatorStateHandle>>();
        const auto& original = prioritizedStates_[currentIndex_++];
        
        // Copy all handles from the original collection
        for (const auto& handle : original) {
            collection->Add(handle);
        }
        
        return collection;
    }
    
    void remove() override {
        // Remove is not supported for OperatorStateHandleIterator
    }
    
private:
    const std::vector<StateObjectCollection<OperatorStateHandle>>& prioritizedStates_;
    size_t currentIndex_;
};
