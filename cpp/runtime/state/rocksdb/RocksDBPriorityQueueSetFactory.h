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

#include "RocksDBCachingPriorityQueueSet.h"
#include "state/PriorityQueueSetFactory.h"
#include "state/RocksDbKvStateInfo.h"
#include "state/RocksDBWriteBatchWrapper.h"
#include "runtime/state/RocksDbKvStateInfo.h"
#include "state/heap/KeyGroupPartitionedPriorityQueue.h"


class RocksDBPriorityQueueSetFactory : public PriorityQueueSetFactory {
public:
    RocksDBPriorityQueueSetFactory(
            KeyGroupRange* keyGroupRange,
            int32_t keyGroupPrefixBytes,
            int32_t numberOfKeyGroups,
            std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation,
            rocksdb::DB* db,
            std::shared_ptr<rocksdb::ReadOptions> readOptions,
            std::shared_ptr<RocksDBWriteBatchWrapper> writeBatchWrapper,
            std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory,
            int64_t writeBufferManagerCapacity)
            :
            keyGroupRange_(keyGroupRange),
            keyGroupPrefixBytes_(keyGroupPrefixBytes),
            numberOfKeyGroups_(numberOfKeyGroups),
            kvStateInformation_(kvStateInformation),
            db_(db),
            readOptions_(readOptions),
            writeBatchWrapper_(writeBatchWrapper),
            columnFamilyOptionsFactory_(columnFamilyOptionsFactory),
            writeBufferManagerCapacity_(writeBufferManagerCapacity) {
        sharedElementOutView_ = std::make_shared<DataOutputSerializer>(128);
        sharedElementInView_ = std::make_shared<DataInputDeserializer>();
    }

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> create(
            std::string stateName,
            TypeSerializer* byteOrderedElementSerializer) {
        return this->create<K, T, Comparator>(stateName, byteOrderedElementSerializer, false);
    }

    template <typename K, typename T, typename Comparator>
    std::shared_ptr<KeyGroupedInternalPriorityQueue<T>> create(
            std::string stateName,
            TypeSerializer* byteOrderedElementSerializer,
            bool allowFutureMetadataUpdates) {
        auto stateCFHandle = tryRegisterPriorityQueueMetaInfo(
                stateName,
                byteOrderedElementSerializer,
                allowFutureMetadataUpdates);
        auto columnFamilyHandle = stateCFHandle->columnFamilyHandle_;
        using Factory = typename KeyGroupPartitionedPriorityQueue<K, T, Comparator>::PartitionQueueSetFactory;
        Factory factory = [this, columnFamilyHandle, byteOrderedElementSerializer](int32_t keyGroupId, int32_t totalKeyGroups) {
            return std::make_shared<RocksDBCachingPriorityQueueSet<K, T, Comparator>>(
                    keyGroupId,
                    keyGroupPrefixBytes_,
                    db_,
                    readOptions_,
                    columnFamilyHandle,
                    byteOrderedElementSerializer,
                    sharedElementOutView_,
                    sharedElementInView_,
                    writeBatchWrapper_,
                    DEFAULT_CACHES_SIZE);
        };
        return std::make_shared<KeyGroupPartitionedPriorityQueue<K, T, Comparator>>(
            factory,
            keyGroupRange_,
            numberOfKeyGroups_);
    }

private:
    std::shared_ptr<RocksDbKvStateInfo> tryRegisterPriorityQueueMetaInfo(
            const std::string& stateName,
            TypeSerializer* byteOrderedElementSerializer,
            bool allowFutureMetadataUpdates) {
        std::shared_ptr<RocksDbKvStateInfo> stateInfo;
        auto iter = kvStateInformation_->find(stateName);
        if (iter == kvStateInformation_->end()) {
            auto metaInfo = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(
                    stateName, byteOrderedElementSerializer);
            metaInfo = allowFutureMetadataUpdates ? metaInfo->withSerializerUpgradesAllowed() : metaInfo;
            stateInfo = RocksDbOperationUtils::createStateInfo(metaInfo, db_, columnFamilyOptionsFactory_);
            RocksDbOperationUtils::registerKvStateInformation(kvStateInformation_, stateName, stateInfo);
        } else {
            stateInfo = iter->second;
            auto castedMetaInfo = std::dynamic_pointer_cast<RegisteredPriorityQueueStateBackendMetaInfo>(stateInfo->metaInfo_);
            auto previousElementSerializer = castedMetaInfo->getPreviousElementSerializer();
            if (previousElementSerializer != byteOrderedElementSerializer) {
                auto compatibilityResult = castedMetaInfo->updateElementSerializer(byteOrderedElementSerializer);
                if (compatibilityResult.isIncompatible()) {
                    THROW_RUNTIME_ERROR("The new priority queue serializer must not be incompatible.")
                }
                auto metaInfo = std::make_shared<RegisteredPriorityQueueStateBackendMetaInfo>(
                        stateName, byteOrderedElementSerializer);
                metaInfo = allowFutureMetadataUpdates ? metaInfo->withSerializerUpgradesAllowed() : metaInfo;
                stateInfo = std::make_shared<RocksDbKvStateInfo>(stateInfo->columnFamilyHandle_, metaInfo);
                kvStateInformation_->emplace(stateName, stateInfo);
            }
        }
        return stateInfo;
    }

    static constexpr int32_t DEFAULT_CACHES_SIZE = 128;
    KeyGroupRange* keyGroupRange_;
    int32_t keyGroupPrefixBytes_;
    int32_t numberOfKeyGroups_;
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>* kvStateInformation_;
    rocksdb::DB* db_;
    std::shared_ptr<rocksdb::ReadOptions> readOptions_;
    std::shared_ptr<RocksDBWriteBatchWrapper> writeBatchWrapper_;
    std::function<rocksdb::ColumnFamilyOptions(const std::string&)> columnFamilyOptionsFactory_;
    int64_t writeBufferManagerCapacity_;

    std::shared_ptr<DataOutputSerializer> sharedElementOutView_;
    std::shared_ptr<DataInputDeserializer> sharedElementInView_;
};
