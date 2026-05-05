#pragma once

#include "KeyedStateHandlesIterator.h"
#include "AbstractStateStreamIterator.h"
#include "core/utils/Iterator.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/KeyGroupStatePartitionStreamProvider.h"
#include <utility>  // for std::pair

using namespace omnistream::utils;

/**
 * Iterator that converts KeyGroupsStateHandle to KeyGroupStatePartitionStreamProvider.
 */
class KeyGroupStreamIterator : 
    public AbstractStateStreamIterator<
        std::shared_ptr<KeyGroupStatePartitionStreamProvider>, 
        KeyGroupsStateHandle> {
private:
    std::unique_ptr<Iterator<std::shared_ptr<std::pair<int, long>>>> currentOffsetsIterator;
    
    // KeyedStateCheckpointOutputStream的静态常量
    static constexpr long NO_OFFSET_SET = -1;
    
    // 辅助方法：跳过未设置的偏移量
    std::unique_ptr<Iterator<std::shared_ptr<std::pair<int, long>>>> 
    unsetOffsetsSkippingIterator(
            std::shared_ptr<KeyGroupsStateHandle> keyGroupsStateHandle) {
        // 获取键组范围偏移量
        const auto& groupRangeOffsets = keyGroupsStateHandle->getGroupRangeOffsets();
        
        // 获取所有偏移量
        const auto& offsets = groupRangeOffsets.getOffsets();
        
        // 获取键组范围
        const auto& keyGroupRange = groupRangeOffsets.getKeyGroupRange();
        
        // 创建一个过滤后的偏移量向量
        std::vector<std::shared_ptr<std::pair<int, long>>> filteredOffsets;
        
        // 遍历所有键组和偏移量
        int startKeyGroup = keyGroupRange.getStartKeyGroup();
        for (size_t i = 0; i < offsets.size(); ++i) {
            int keyGroupId = startKeyGroup + static_cast<int>(i);
            long offset = offsets[i];
            if (offset != NO_OFFSET_SET) {
                filteredOffsets.push_back(std::make_shared<std::pair<int, long>>(keyGroupId, offset));
            }
        }
        
        // 返回过滤后的偏移量迭代器
        return std::make_unique<VectorIterator<std::pair<int, long>>>(filteredOffsets);
    };
    
public:
    KeyGroupStreamIterator(
            std::unique_ptr<Iterator<std::shared_ptr<KeyGroupsStateHandle>>> stateHandleIterator)
        : AbstractStateStreamIterator(std::move(stateHandleIterator)),
        currentOffsetsIterator() {}
    ~KeyGroupStreamIterator() override = default;
    
    // hasNext()方法实现
    bool hasNext() {
        // 检查当前状态句柄和偏移量迭代器是否还有下一个元素
        if (currentStateHandle && currentOffsetsIterator != nullptr && currentOffsetsIterator->hasNext()) {
            return true;
        }
        
        // 关闭当前流
        closeCurrentStream();
        
        // 遍历所有状态句柄
        while (stateHandleIterator->hasNext()) {
            currentStateHandle = stateHandleIterator->next();
            
            // 检查键组范围是否有键组
            if (currentStateHandle->GetKeyGroupRange().getNumberOfKeyGroups() > 0) {
                // 获取跳过未设置偏移量的迭代器
                currentOffsetsIterator = unsetOffsetsSkippingIterator(currentStateHandle);
                
                // 检查是否有下一个元素
                if (currentOffsetsIterator != nullptr && currentOffsetsIterator->hasNext()) {
                    return true;
                }
            }
        }
        
        // 没有更多元素
        return false;
    }
    

    std::shared_ptr<KeyGroupStatePartitionStreamProvider> next() {
        // 确保有下一个元素
        if (!hasNext()) {
            throw std::runtime_error("Iterator exhausted");
        }
        
        // 确保currentOffsetsIterator不为空
        if (currentOffsetsIterator == nullptr) {
            throw std::runtime_error("currentOffsetsIterator is null");
        }
        
        // 获取下一个键组偏移量
        auto keyGroupOffset = currentOffsetsIterator->next();
        
        try {
            // 如果当前流未打开，则打开它
            if (!currentStream) {
                openCurrentStream();
            }
            
            // 跳转到指定偏移量位置
            currentStream->Seek(keyGroupOffset->second);
            
            // 创建并返回KeyGroupStatePartitionStreamProvider实例
            return std::make_shared<KeyGroupStatePartitionStreamProvider>(
                currentStream, keyGroupOffset->first);
        } catch (const std::exception& e) {
            // 如果发生异常，返回包含异常的KeyGroupStatePartitionStreamProvider实例
            return std::make_shared<KeyGroupStatePartitionStreamProvider>(
                std::current_exception(), keyGroupOffset->first);
        }
    }

    void remove() override {
    // Remove is not supported for KeyGroupStreamIterator
    }
};