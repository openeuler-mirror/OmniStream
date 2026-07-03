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
#include <vector>

#include "utils/Iterator.h"

template <typename T>
class InternalPriorityQueue {
public:
    virtual ~InternalPriorityQueue() = default;

    // Retrieves and removes the first element, returns nullptr if the queue is empty
    virtual T poll() = 0;

    // Retrieves the first element, returns nullptr if the queue is empty
    virtual T peek() = 0;

    // Adds an element to the queue, returns false if the head of the queue is not changed by this operation
    virtual bool add(const T& element) = 0;

    // Removes the specified element from the queue, returns false if the head of the queue is not changed by this
    // operation
    virtual bool remove(const T& element) = 0;

    virtual bool isEmpty() = 0;

    virtual int32_t size() = 0;

    virtual void addAll(const std::vector<T>& elements) = 0;

    virtual std::unique_ptr<omnistream::utils::Iterator<T>> iterator()
    {
        NOT_IMPL_EXCEPTION;
    };
};
