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

#ifndef FLINK_TNEL_ABSTRACTSTATESTREAMITERATOR_H
#define FLINK_TNEL_ABSTRACTSTATESTREAMITERATOR_H

#include "core/utils/Iterator.h"
#include "core/fs/FSDataInputStream.h"

using namespace omnistream::utils;

/**
 * Abstract base class for state stream iterators.
 * Provides common functionality for iterating over state handles and managing streams.
 * 
 * @tparam T The type of elements returned by the iterator
 * @tparam H The type of state handle
 */
template <typename T, typename H>
class AbstractStateStreamIterator : public Iterator<T> {
protected:
    std::unique_ptr<Iterator<std::shared_ptr<H>>> stateHandleIterator;
    std::shared_ptr<H> currentStateHandle;
    std::shared_ptr<FSDataInputStream> currentStream;
    
protected:
    void openCurrentStream() {
        // 实现打开当前流的逻辑
        // 这里假设currentStateHandle有一个openInputStream方法
        currentStream = currentStateHandle->OpenInputStream();
    }
    
    void closeCurrentStream() {
        if (currentStream) {
            currentStream->Close();
            currentStream = nullptr;
        }
    }
    
public:
    AbstractStateStreamIterator(
            std::unique_ptr<Iterator<std::shared_ptr<H>>> stateHandleIterator)
        : stateHandleIterator(std::move(stateHandleIterator)),
          currentStateHandle(nullptr),
          currentStream(nullptr) {}
    
    virtual ~AbstractStateStreamIterator() {
        closeCurrentStream();
    }
};

#endif // FLINK_TNEL_ABSTRACTSTATESTREAMITERATOR_H
