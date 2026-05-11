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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "HeapPriorityQueueSnapshotRestoreWrapper.h"
#include "HeapPriorityQueueSnapshotRestoreWrapperBase.h"
#include "runtime/state/RegisteredPriorityQueueStateBackendMetaInfo.h"
#include "runtime/state/rocksdb/iterator/SingleStateIterator.h"

/**
 * A temporary restore wrapper for RocksDB savepoint PRIORITY_QUEUE state.
 *
 * During RocksDB heap-timer savepoint restore, the full snapshot stream contains
 * PRIORITY_QUEUE entries before the typed InternalTimerService is recreated.
 * Unlike Java, this C++ code does not know the template namespace/comparator at
 * metadata restore time, so we first keep the serialized entries here and drain
 * them into the real HeapPriorityQueueSnapshotRestoreWrapper once
 * HeapPriorityQueuesManager::createOrUpdate(...) is called with the concrete
 * timer type.
 */
class RestoredHeapPriorityQueueSnapshotRestoreWrapper : public HeapPriorityQueueSnapshotRestoreWrapperBase {
private:
    class PendingSingleStateIterator : public SingleStateIterator {
    public:
        PendingSingleStateIterator(int kvStateId, const std::vector<std::vector<int8_t>> &serializedKeys)
            : kvStateId_(kvStateId), serializedKeys_(serializedKeys), valid_(!serializedKeys_.empty()) {}

        void next() override
        {
            if (valid_) {
                currentIndex_++;
                valid_ = currentIndex_ < serializedKeys_.size();
            }
        }

        bool isValid() const override
        {
            return valid_;
        }

        std::vector<int8_t> key() const override
        {
            return serializedKeys_[currentIndex_];
        }

        std::vector<int8_t> value() const override
        {
            return {};
        }

        int getKvStateId() const override
        {
            return kvStateId_;
        }

        void close() override
        {
            valid_ = false;
            serializedKeys_.clear();
        }

    private:
        int kvStateId_;
        std::vector<std::vector<int8_t>> serializedKeys_;
        size_t currentIndex_ = 0;
        bool valid_ = false;
    };

public:
    explicit RestoredHeapPriorityQueueSnapshotRestoreWrapper(
        std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo)
        : metaInfo_(std::move(metaInfo)) {}

    std::shared_ptr<StateMetaInfoSnapshot> snapshotMetaInfo() override
    {
        return metaInfo_ == nullptr ? nullptr : metaInfo_->snapshot();
    }

    std::unique_ptr<SingleStateIterator> createSnapshotIterator(
        int kvStateId,
        int /*keyGroupPrefixBytes*/) override
    {
        return std::make_unique<PendingSingleStateIterator>(kvStateId, serializedKeys_);
    }

    void restoreSerializedElement(
        const std::vector<int8_t> &serializedKey,
        int /*keyGroupPrefixBytes*/) override
    {
        if (!serializedKey.empty()) {
            serializedKeys_.push_back(serializedKey);
        }
    }

    bool empty() const
    {
        return serializedKeys_.empty();
    }

    size_t size() const
    {
        return serializedKeys_.size();
    }

    const std::string &getStateName() const
    {
        static const std::string emptyName;
        return metaInfo_ == nullptr ? emptyName : metaInfo_->getName();
    }

    template <typename K, typename T, typename Comparator>
    void drainTo(
        const std::shared_ptr<HeapPriorityQueueSnapshotRestoreWrapper<K, T, Comparator>> &target,
        int keyGroupPrefixBytes)
    {
        if (target == nullptr) {
            INFO_RELEASE("Error: drainTo Cannot drain restored priority queue entries to null target");
            THROW_LOGIC_EXCEPTION("Cannot drain restored priority queue entries to null target")
        }

        for (const auto &serializedKey : serializedKeys_) {
            target->restoreSerializedElement(serializedKey, keyGroupPrefixBytes);
        }
        serializedKeys_.clear();
    }

private:
    std::shared_ptr<RegisteredPriorityQueueStateBackendMetaInfo> metaInfo_;
    std::vector<std::vector<int8_t>> serializedKeys_;
};
