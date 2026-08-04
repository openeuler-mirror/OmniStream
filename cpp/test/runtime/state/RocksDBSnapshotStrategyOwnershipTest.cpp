/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/CheckpointType.h"
#include "runtime/snapshot/RocksIncrementalSnapshotStrategy.h"
#include "runtime/snapshot/RocksNativeFullSnapshotStrategy.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

namespace {

std::shared_ptr<NativeRocksDBSnapshotResources> makeResources()
{
    auto metaInfo = std::make_shared<StateMetaInfoSnapshot>(
        "state",
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
    return std::make_shared<NativeRocksDBSnapshotResources>(
        nullptr,
        PreviousSnapshot::EMPTY_PREVIOUS_SNAPSHOT,
        std::vector<std::shared_ptr<StateMetaInfoSnapshot>>{metaInfo});
}

CheckpointOptions makeCheckpointOptions()
{
    return CheckpointOptions(CheckpointType::CHECKPOINT, CheckpointStorageLocationReference::GetDefault());
}

} // namespace

TEST(RocksDBSnapshotStrategyOwnershipTest, IncrementalAsyncSupplierRetainsConcreteStrategy)
{
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation;
    std::map<long, std::vector<HandleAndLocalPath>> uploadedStateHandles;
    auto resources = makeResources();
    auto checkpointOptions = makeCheckpointOptions();
    auto uploader = std::make_shared<RocksDBStateUploader>(1);
    std::weak_ptr<RocksIncrementalSnapshotStrategy> weakStrategy;
    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> supplier;

    {
        auto strategy = std::make_shared<RocksIncrementalSnapshotStrategy>(
            nullptr,
            std::make_shared<ResourceGuard>(),
            nullptr,
            &kvStateInformation,
            KeyGroupRange(),
            0,
            std::make_shared<LocalRecoveryConfig>(nullptr),
            "",
            UUID(),
            uploadedStateHandles,
            uploader,
            -1L);
        weakStrategy = strategy;
        supplier = strategy->asyncSnapshot(resources, 1L, 1L, nullptr, &checkpointOptions, "");
        ASSERT_NE(supplier, nullptr);
        strategy.reset();
        EXPECT_FALSE(weakStrategy.expired());
    }

    supplier.reset();
    EXPECT_TRUE(weakStrategy.expired());
}

TEST(RocksDBSnapshotStrategyOwnershipTest, NativeFullAsyncSupplierRetainsConcreteStrategy)
{
    std::unordered_map<std::string, std::shared_ptr<RocksDbKvStateInfo>> kvStateInformation;
    auto resources = makeResources();
    auto checkpointOptions = makeCheckpointOptions();
    auto uploader = std::make_shared<RocksDBStateUploader>(1);
    std::weak_ptr<RocksNativeFullSnapshotStrategy> weakStrategy;
    std::shared_ptr<SnapshotResultSupplier<KeyedStateHandle>> supplier;

    {
        auto strategy = std::make_shared<RocksNativeFullSnapshotStrategy>(
            nullptr,
            std::make_shared<ResourceGuard>(),
            nullptr,
            &kvStateInformation,
            KeyGroupRange(),
            0,
            std::make_shared<LocalRecoveryConfig>(nullptr),
            "",
            UUID(),
            uploader);
        weakStrategy = strategy;
        supplier = strategy->asyncSnapshot(resources, 1L, 1L, nullptr, &checkpointOptions, "");
        ASSERT_NE(supplier, nullptr);
        strategy.reset();
        EXPECT_FALSE(weakStrategy.expired());
    }

    supplier.reset();
    EXPECT_TRUE(weakStrategy.expired());
}
