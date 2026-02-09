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

#ifndef OMNISTREAM_ROCKSDBFULLSNAPSHOTRESOURCES_H
#define OMNISTREAM_ROCKSDBFULLSNAPSHOTRESOURCES_H
#include "fs/CloseableRegistry.h"
#include "state/FullSnapshotResources.h"
#include "state/KeyGroupRange.h"
#include "state/KeyValueStateIterator.h"
#include "state/RocksIteratorWrapper.h"
#include "state/RocksDbKvStateInfo.h"
#include "state/StateSnapshotTransformer.h"
#include "state/metainfo/StateMetaInfoSnapshot.h"
#include "state/rocksdb/util/ResourceGuard.h"
#include "typeutils/TypeSerializer.h"
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/snapshot.h>
#include <vector>
#include <cstdint>
class RocksDBFullSnapshotResources : public FullSnapshotResources {
private:
    struct MetaData {
        std::shared_ptr<RocksDbKvStateInfo> rocksDbKvStateInfo;
        std::shared_ptr<StateSnapshotTransformer<std::vector<int8_t>>>
            stateSnapshotTransformer;
        MetaData(
            const std::shared_ptr<RocksDbKvStateInfo>& info,
            const std::shared_ptr<StateSnapshotTransformer<std::vector<int8_t>>>& transformer)
            : rocksDbKvStateInfo(info), stateSnapshotTransformer(transformer)
        {
        }
    };
private:
    ResourceGuard::Lease* lease_;
    const rocksdb::Snapshot* snapshot_;
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> stateMetaInfoSnapshots_;
    rocksdb::DB* db_;
    std::vector<std::shared_ptr<MetaData>> metaData_;
    int keyGroupPrefixBytes_;
    KeyGroupRange* keyGroupRange_;
    TypeSerializer* keySerializer_;
public:
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>&
    getMetaInfoSnapshots() override;
    KeyGroupRange* getKeyGroupRange() override;
    TypeSerializer* getKeySerializer() override;
    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override;
    std::vector<std::pair<std::unique_ptr<RocksIteratorWrapper>, int>>
    createKVStateIterators(
        std::unique_ptr<CloseableRegistry>& closeableRegistry,
        const rocksdb::ReadOptions& readOptions);

    std::unique_ptr<RocksIteratorWrapper>
    createRocksIteratorWrapper(
        rocksdb::DB* db,
        rocksdb::ColumnFamilyHandle* columnFamilyHandle,
        StateSnapshotTransformer<std::vector<int8_t>>* stateSnapshotTransformer,
        const rocksdb::ReadOptions& readOptions);

    RocksDBFullSnapshotResources(
        ResourceGuard::Lease* lease,
        const rocksdb::Snapshot* snapshot,
        const std::vector<std::shared_ptr<RocksDbKvStateInfo>>& metaDataCopy,
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& stateMetaInfoSnapshots,
        rocksdb::DB* db,
        int keyGroupPrefixBytes,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer);
        
    static std::shared_ptr<RocksDBFullSnapshotResources>
    create(
        const std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>>& kvStateInformation,
        rocksdb::DB* db,
        const std::shared_ptr<ResourceGuard>& rocksDBResourceGuard,
        KeyGroupRange* keyGroupRange,
        TypeSerializer* keySerializer,
        int keyGroupPrefixBytes);
};
#endif