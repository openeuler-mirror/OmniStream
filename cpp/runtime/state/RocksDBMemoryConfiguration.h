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

class RocksDBMemoryConfiguration {
public:
    bool isUsingManagedMemory() const {
        return usingManagedMemory;
    }

    double getWriteBufferRatio() const {
        return writeBufferRatio;
    }

    double getHighPriorityPoolRatio() const {
        return highPriorityPoolRatio;
    }

    bool isUsingPartitionedIndexFilters() const {
        return usingPartitionedIndexFilters;
    }

    bool isUsingFixedMemoryPerSlot() const {
        return usingFixedMemoryPerSlot;
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(RocksDBMemoryConfiguration, usingManagedMemory, writeBufferRatio, highPriorityPoolRatio, usingPartitionedIndexFilters, usingFixedMemoryPerSlot)
private:
    bool usingManagedMemory;
    // todo: MemorySize fixedMemoryPerSlot
    double writeBufferRatio;
    double highPriorityPoolRatio;
    bool usingPartitionedIndexFilters;
    bool usingFixedMemoryPerSlot;
};
