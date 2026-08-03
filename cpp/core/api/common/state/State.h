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

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "common.h"

namespace omnistream {
class VectorBatch;
enum class StateType {
    UNKNOWN = -1,
    HEAP = 0,
    ROCKSDB = 1,
    BSS = 2
};

void checkStateType(StateType stateType, const std::string& description);
} // namespace omnistream

class State {
public:
    State() = default;

    virtual ~State() = default;

    virtual void clear() = 0;

    // only used for VectorBatch storage
    virtual uint32_t getNextSequenceNumber(int32_t keyGroup)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual void addVectorBatch(int32_t keyGroup, omnistream::VectorBatch* vectorBatch)
    {
        NOT_IMPL_EXCEPTION;
    }

    /**
     *
     * @param vectorBatchByKeyGroup the key is the keyGroup of the VectorBatch
     */
    virtual void addVectorBatches(const std::unordered_map<int32_t, omnistream::VectorBatch*>& vectorBatchByKeyGroup)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual omnistream::VectorBatch* getVectorBatch(int32_t keyGroup, uint32_t sequenceNumber)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual std::vector<omnistream::VectorBatch*> getVectorBatches(int32_t keyGroup)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual void clearVectorBatches(int64_t currentTimestamp)
    {
        NOT_IMPL_EXCEPTION;
    }

    virtual void clearVectorBatches(int32_t keyGroup, std::vector<uint32_t>& sequenceNumbersToDelete)
    {
        NOT_IMPL_EXCEPTION;
    }
};
