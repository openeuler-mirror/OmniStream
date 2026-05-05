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

#ifndef FLINK_TNEL_RAWOPERATORSTATEINPUTSITERABLE_H
#define FLINK_TNEL_RAWOPERATORSTATEINPUTSITERABLE_H

#include "core/utils/Iterator.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/state/KeyGroupStatePartitionStreamProvider.h"
#include "OperatorStateStreamIterator.h"

using namespace omnistream::utils;

/**
 * Iterable for raw operator state inputs.
 * This class provides an iterator that converts OperatorStateHandle objects
 * to StatePartitionStreamProvider objects.
 */
class RawOperatorStateInputsIterable : public Iterable<std::shared_ptr<StatePartitionStreamProvider>> {
public:
    /**
     * Constructor.
     * @param operatorStateName The name of the operator state.
     * @param operatorStateHandles Vector of OperatorStateHandle objects.
     */
    RawOperatorStateInputsIterable(
        const std::string& operatorStateName,
        const std::vector<std::shared_ptr<OperatorStateHandle>>& operatorStateHandles)
        : operatorStateName_(operatorStateName),
          operatorStateHandles_(operatorStateHandles) {}
    
    /**
     * Returns an iterator over StatePartitionStreamProvider objects.
     * @return Unique pointer to the iterator.
     */
    std::unique_ptr<Iterator<std::shared_ptr<StatePartitionStreamProvider>>> iterator() override {
        // Create a VectorIterator for the operatorStateHandles with explicit type conversion
        std::unique_ptr<Iterator<std::shared_ptr<OperatorStateHandle>>> vectorIter = 
            std::make_unique<VectorIterator<OperatorStateHandle>>(operatorStateHandles_);
        
        // Create an OperatorStateStreamIterator that wraps the VectorIterator
        return std::make_unique<OperatorStateStreamIterator>(operatorStateName_, std::move(vectorIter));
    }
    
private:
    const std::string operatorStateName_;
    const std::vector<std::shared_ptr<OperatorStateHandle>>& operatorStateHandles_;
};

#endif // FLINK_TNEL_RAWOPERATORSTATEINPUTSITERABLE_H
