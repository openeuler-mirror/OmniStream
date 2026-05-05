#pragma once

#include "core/utils/Iterator.h"
#include "runtime/state/KeyedStateHandle.h"
#include "runtime/checkpoint/StateObjectCollection.h"

using namespace omnistream::utils;

/**
 * Iterator for iterating over prioritized raw keyed state collections.
 */
class KeyedStateHandlesIterator : public omnistream::utils::Iterator<std::shared_ptr<StateObjectCollection<KeyedStateHandle>>> {
public:
    KeyedStateHandlesIterator(const std::vector<StateObjectCollection<KeyedStateHandle>>& prioritizedStates)
        : prioritizedStates_(prioritizedStates),
          currentIndex_(0) {}
    
    ~KeyedStateHandlesIterator() override = default;
    
    bool hasNext() override {
        return currentIndex_ < prioritizedStates_.size();
    }
    
    std::shared_ptr<StateObjectCollection<KeyedStateHandle>> next() override {
        if (!hasNext()) {
            return nullptr;
        }
        
        // Create a new shared_ptr to avoid ownership issues
        auto collection = std::make_shared<StateObjectCollection<KeyedStateHandle>>();
        const auto& original = prioritizedStates_[currentIndex_++];
        
        // Copy all handles from the original collection
        for (const auto& handle : original) {
            collection->Add(handle);
        }
        
        return collection;
    }
    
    void remove() override {
        // Remove is not supported for KeyedStateHandlesIterator
    }
    
private:
    const std::vector<StateObjectCollection<KeyedStateHandle>>& prioritizedStates_;
    size_t currentIndex_;
};