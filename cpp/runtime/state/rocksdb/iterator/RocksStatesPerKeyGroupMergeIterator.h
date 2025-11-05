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
#ifndef OMNISTREAM_ROCKSSTATESPERKEYGROUPMERGEITERATOR_H
#define OMNISTREAM_ROCKSSTATESPERKEYGROUPMERGEITERATOR_H

#include <queue>
#include <vector>
#include <functional>
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <memory>
#include "runtime/state/KeyValueStateIterator.h"
#include "core/fs/CloseableRegistry.h"
#include "runtime/state/RocksIteratorWrapper.h"
#include "SingleStateIterator.h"
#include "RocksSingleStateIterator.h"

class RocksStatesPerKeyGroupMergeIterator : public KeyValueStateIterator {
public:
    // 比较器结构体
    struct IteratorComparator {
        IteratorComparator(int keyGroupPrefixByteCount)
            : keyGroupPrefixByteCount(keyGroupPrefixByteCount) {}

        bool operator()(const std::unique_ptr<SingleStateIterator>& a,
                        const std::unique_ptr<SingleStateIterator>& b) const
        {
            // 先按键组前缀比较
            int cmp = compareKeyGroupsForByteArrays(a->key(), b->key(), keyGroupPrefixByteCount);
            if (cmp != 0) {
                // 最小堆需要反转比较结果
                return cmp > 0;
            }

            // 然后按kvStateId比较
            return a->getKvStateId() > b->getKvStateId();
        }

        int keyGroupPrefixByteCount;
    };

    RocksStatesPerKeyGroupMergeIterator(
        std::unique_ptr<CloseableRegistry> closeableRegistry,
        std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>> kvStateIterators,
        std::vector<std::unique_ptr<SingleStateIterator>> heapPriorityQueueIterators,
        int keyGroupPrefixByteCount)
        : closeableRegistry_(std::move(closeableRegistry)),
        heap_(IteratorComparator(keyGroupPrefixByteCount)),
        keyGroupPrefixByteCount_(keyGroupPrefixByteCount)
    {
        if (keyGroupPrefixByteCount < 1) {
            throw std::invalid_argument("keyGroupPrefixByteCount must be at least 1");
        }

        // 构建迭代器堆
        if (!kvStateIterators.empty() || !heapPriorityQueueIterators.empty()) {
            buildIteratorHeap(kvStateIterators, heapPriorityQueueIterators);
            valid_ = !heap_.empty();
            if (valid_) {
                currentSubIterator_ = std::move(const_cast<std::unique_ptr<SingleStateIterator>&>(heap_.top()));
                heap_.pop();
            }
        } else {
            valid_ = false;
        }

        newKeyGroup_ = true;
        newKVState_ = true;
    }

    void next() override
    {
        newKeyGroup_ = false;
        newKVState_ = false;

        if (!valid_) return;

        // 保存旧键用于比较
        auto oldKey = currentSubIterator_->key();

        // 推进当前迭代器
        currentSubIterator_->next();

        if (currentSubIterator_->isValid()) {
            // 检查键组是否变化
            if (isDifferentKeyGroup(oldKey, currentSubIterator_->key())) {
                SingleStateIterator* oldIteratorPtr = currentSubIterator_.get();
                auto temp = std::move(currentSubIterator_);
                heap_.push(std::move(temp));
                currentSubIterator_ =
                        std::move(const_cast<std::unique_ptr<SingleStateIterator>&>(heap_.top()));
                heap_.pop();
                newKVState_ = (currentSubIterator_.get() != oldIteratorPtr);
                detectNewKeyGroup(oldKey);
            }
        } else {
            // 迭代器已耗尽，关闭并移除
            if (currentSubIterator_) {
                currentSubIterator_->close();
            }

            if (heap_.empty()) {
                currentSubIterator_.reset();
                valid_ = false;
            } else {
                currentSubIterator_ = std::move(const_cast<std::unique_ptr<SingleStateIterator>&>(heap_.top()));
                heap_.pop();
                newKVState_ = true;
                detectNewKeyGroup(oldKey);
            }
        }
    }

    int keyGroup() const override
    {
        const auto& currentKey = currentSubIterator_->key();
        int result = 0;
        // 大端解码
        for (int i = 0; i < keyGroupPrefixByteCount_; ++i) {
            result <<= 8;
            result |= (currentKey[i] & 0xFF);
        }
        return result;
    }

    std::vector<uint8_t> key() const
    {
        return currentSubIterator_->key();
    }

    std::vector<uint8_t> value() const
    {
        return currentSubIterator_->value();
    }

    int kvStateId() const override
    {
        return currentSubIterator_->getKvStateId();
    }

    bool isNewKeyValueState() const override
    {
        return newKVState_;
    }

    bool isNewKeyGroup() const override
    {
        return newKeyGroup_;
    }

    bool isValid() const override
    {
        return valid_;
    }

    void close() override
    {
        if (closeableRegistry_) {
            closeableRegistry_->close();
            closeableRegistry_.reset();
        }

        // 清空堆
        while (!heap_.empty()) {
            heap_.pop();
        }

        if (currentSubIterator_) {
            currentSubIterator_.reset();
        }
    }

    ~RocksStatesPerKeyGroupMergeIterator()
    {
        close();
    }

private:
    std::unique_ptr<CloseableRegistry> closeableRegistry_;
    std::priority_queue<std::unique_ptr<SingleStateIterator>,
        std::vector<std::unique_ptr<SingleStateIterator>>,
        IteratorComparator> heap_;
    std::unique_ptr<SingleStateIterator> currentSubIterator_;
    int keyGroupPrefixByteCount_;
    bool newKeyGroup_ = false;
    bool newKVState_ = false;
    bool valid_ = false;

    void buildIteratorHeap(
        std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>>& kvStateIterators,
        std::vector<std::unique_ptr<SingleStateIterator>>& heapPriorityQueueIterators)
    {
        // 处理键值状态迭代器
        for (auto& iterPair : kvStateIterators) {
            auto& rocksIter = iterPair.first;
            int kvStateId = iterPair.second;

            rocksIter->seekToFirst();
            if (rocksIter->isValid()) {
                auto wrappingIter = std::make_unique<RocksSingleStateIterator>(
                        std::move(rocksIter), kvStateId);

                heap_.push(std::move(wrappingIter));
            } else {
                rocksIter->close();
            }
        }

        // 处理堆优先队列迭代器
        for (auto& heapIter : heapPriorityQueueIterators) {
            if (heapIter->isValid()) {
                heap_.push(std::move(heapIter));
            } else {
                heapIter->close();
            }
        }
    }

    bool isDifferentKeyGroup(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b)
    {
        // 优化：使用memcmp进行快速比较
        if (a.size() < keyGroupPrefixByteCount_ || b.size() < keyGroupPrefixByteCount_) {
            return true;
        }

        return std::memcmp(a.data(), b.data(), keyGroupPrefixByteCount_) != 0;
    }

    void detectNewKeyGroup(const std::vector<uint8_t>& oldKey)
    {
        if (isDifferentKeyGroup(oldKey, currentSubIterator_->key())) {
            newKeyGroup_ = true;
        }
    }

    static int compareKeyGroupsForByteArrays(
        const std::vector<uint8_t>& a,
        const std::vector<uint8_t>& b,
        int len)
    {
        // 确保有足够的字节进行比较
        size_t inLen = std::min({a.size(), b.size(), static_cast<size_t>(len)});

        for (int i = 0; i < inLen; ++i) {
            int diff = static_cast<int>(a[i]) - static_cast<int>(b[i]);
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }
};
#endif // OMNISTREAM_ROCKSSTATESPERKEYGROUPMERGEITERATOR_H
