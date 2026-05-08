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

class HeapPriorityQueueElement {
public:
    static constexpr int32_t NOT_CONTAINED = INT32_MIN;

    virtual ~HeapPriorityQueueElement() = default;

    virtual int32_t getInternalIndex() {
        return internalIndex_;
    }

    virtual void setInternalIndex(int32_t index) {
        internalIndex_ = index;
    }

private:
    int32_t internalIndex_ = NOT_CONTAINED;
};