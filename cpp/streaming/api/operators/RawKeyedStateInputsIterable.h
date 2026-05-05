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

#ifndef FLINK_TNEL_RAWKEYEDSTATEINPUTSITERABLE_H
#define FLINK_TNEL_RAWKEYEDSTATEINPUTSITERABLE_H

#include "core/utils/Iterator.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyGroupStatePartitionStreamProvider.h"
#include "KeyGroupStreamIterator.h"

using omnistream::utils::Iterable;
using omnistream::utils::Iterator;
using omnistream::utils::VectorIterator;

/**
 * Iterable for raw keyed state inputs.
 * This class provides an iterator that converts KeyGroupsStateHandle objects
 * to KeyGroupStatePartitionStreamProvider objects.
 */
class RawKeyedStateInputsIterable : public Iterable<std::shared_ptr<KeyGroupStatePartitionStreamProvider>> {
public:
    /**
     * Constructor.
     * @param keyGroupsHandles Vector of KeyGroupsStateHandle objects.
     */
    RawKeyedStateInputsIterable(
        const std::vector<std::shared_ptr<KeyGroupsStateHandle>>& keyGroupsHandles)
        : keyGroupsHandles_(keyGroupsHandles) {}
    
    /**
     * Returns an iterator over KeyGroupStatePartitionStreamProvider objects.
     * @return Unique pointer to the iterator.
     */
    std::unique_ptr<Iterator<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>> iterator() override {
        // Create a VectorIterator for the keyGroupsHandles
        auto vectorIter = std::make_unique<VectorIterator<KeyGroupsStateHandle>>(keyGroupsHandles_);
        
        // Create a KeyGroupStreamIterator that wraps the VectorIterator
        return std::unique_ptr<Iterator<std::shared_ptr<KeyGroupStatePartitionStreamProvider>>>(new KeyGroupStreamIterator(std::move(vectorIter)));
    }
    
private:
    const std::vector<std::shared_ptr<KeyGroupsStateHandle>>& keyGroupsHandles_;
};

#endif // FLINK_TNEL_RAWKEYEDSTATEINPUTSITERABLE_H
