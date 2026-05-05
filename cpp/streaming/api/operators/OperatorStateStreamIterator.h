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

#ifndef FLINK_TNEL_OPERATORSTATESTREAMITERATOR_H
#define FLINK_TNEL_OPERATORSTATESTREAMITERATOR_H

#include "core/utils/Iterator.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/state/KeyGroupStatePartitionStreamProvider.h"
#include <stdexcept>
#include <memory>
#include <iostream>
#include <sstream>

using namespace omnistream::utils;

// Precondition checking macro
#define CHECK_NOT_NULL(ptr, message) \
    if (!(ptr)) { \
        throw std::invalid_argument(message); \
    }

/**
 * Iterator that converts OperatorStateHandle to StatePartitionStreamProvider.
 */

 class OperatorStateStreamIterator : 
    public AbstractStateStreamIterator<
        std::shared_ptr<StatePartitionStreamProvider>, 
        OperatorStateHandle> {
private:
    std::string stateName; // TODO since we only support a single named state in raw, this could be dropped
    std::vector<long> offsets;
    size_t offPos;
    
public:
    OperatorStateStreamIterator(
            const std::string& stateName,
            std::unique_ptr<Iterator<std::shared_ptr<OperatorStateHandle>>> stateHandleIterator)
        : AbstractStateStreamIterator(std::move(stateHandleIterator)),
        stateName(stateName),
        offPos(0) {
        if (stateName.empty()) {
            throw std::invalid_argument("State name cannot be null or empty");
        }
    }

    ~OperatorStateStreamIterator() override = default;

    // hasNext()方法实现
    bool hasNext() {
        // 检查当前偏移量数组和位置
        if (!offsets.empty() && offPos < offsets.size()) {
            return true;
        }
        
        // 关闭当前流
        closeCurrentStream();
        
        // 遍历所有状态句柄
        while (stateHandleIterator->hasNext()) {
            currentStateHandle = stateHandleIterator->next();
            
            // 获取状态元信息
            auto metaInfo = currentStateHandle->getStateNameToPartitionOffsets().find(stateName);
            
            if (metaInfo != currentStateHandle->getStateNameToPartitionOffsets().end()) {
                const auto& metaOffsets = metaInfo->second.getOffsets();
                if (!metaOffsets.empty()) {
                    // 设置偏移量数组和位置
                    offsets = metaOffsets;
                    offPos = 0;
                    try {
                        currentStream->Close();
                    } catch (...) {
                        // 忽略关闭异常
                    }
                    currentStream = nullptr;
                    return true;
                }
            }
        }
        
        // 没有更多元素
        return false;
    }
    
    // next()方法实现
    std::shared_ptr<StatePartitionStreamProvider> next() {
        // 确保有下一个元素
        if (!hasNext()) {
            throw std::runtime_error("Iterator exhausted");
        }
        
        // 获取当前偏移量并递增位置
        long offset = offsets[offPos++];
        
        try {
            // 如果当前流未打开，则打开它
            if (!currentStream) {
                openCurrentStream();
            }
            
            // 跳转到指定偏移量位置
            currentStream->Seek(offset);
            
            // 创建并返回StatePartitionStreamProvider实例
            return std::make_shared<StatePartitionStreamProvider>(currentStream);
        } catch (const std::exception& e) {
            // 如果发生异常，返回包含异常的StatePartitionStreamProvider实例
            return std::make_shared<StatePartitionStreamProvider>(std::current_exception());
        }
    }


    void remove() override {
        // Remove is not supported for OperatorStateStreamIterator
    }
};

#endif // FLINK_TNEL_OPERATORSTATESTREAMITERATOR_H
