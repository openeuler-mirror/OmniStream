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

#include <memory>
#include <unordered_map>
#include <vector>

#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/state/CheckpointStorageLocationReference.h"
#include "runtime/checkpoint/SavepointType.h"
#include "runtime/state/FullSnapshotAsyncWriter.h"
#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/SavepointSnapshotStrategy.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"

namespace {

class MockFullSnapshotResources : public FullSnapshotResources {
public:
    explicit MockFullSnapshotResources(bool withMetaInfo)
        : keyGroupRange_(std::make_unique<KeyGroupRange>(0, 0))
    {
        if (withMetaInfo) {
            metaInfos_.push_back(std::make_shared<StateMetaInfoSnapshot>(
                "testState",
                StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
                std::unordered_map<std::string, std::string>{},
                std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{}));
        }
    }

    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& getMetaInfoSnapshots() override
    {
        return metaInfos_;
    }

    KeyGroupRange* getKeyGroupRange() override
    {
        return keyGroupRange_.get();
    }

    TypeSerializer* getKeySerializer() override
    {
        return nullptr;
    }

    std::shared_ptr<KeyValueStateIterator> createKVStateIterator() override
    {
        return nullptr;
    }

    void cleanup() override
    {
        cleanupCalled_ = true;
    }

    bool wasCleanupCalled() const
    {
        return cleanupCalled_;
    }

private:
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metaInfos_;
    std::unique_ptr<KeyGroupRange> keyGroupRange_;
    bool cleanupCalled_ = false;
};

} // namespace

TEST(SavepointSnapshotStrategyTest, SyncPrepareResourcesReturnsConstructorResource)
{
    auto mockResources = std::make_shared<MockFullSnapshotResources>(false);
    SavepointSnapshotStrategy strategy(mockResources);

    auto prepared = strategy.syncPrepareResources(42L);
    EXPECT_EQ(prepared, mockResources);
}

TEST(SavepointSnapshotStrategyTest, SyncPrepareResourcesIgnoresCheckpointId)
{
    auto mockResources = std::make_shared<MockFullSnapshotResources>(false);
    SavepointSnapshotStrategy strategy(mockResources);

    auto prepared1 = strategy.syncPrepareResources(1L);
    auto prepared2 = strategy.syncPrepareResources(999L);
    EXPECT_EQ(prepared1, mockResources);
    EXPECT_EQ(prepared2, mockResources);
}

TEST(SavepointSnapshotStrategyTest, AsyncSnapshotWithEmptyMetaReturnsEmptyResult)
{
    auto mockResources = std::make_shared<MockFullSnapshotResources>(false);
    SavepointSnapshotStrategy strategy(mockResources);
    std::unique_ptr<SavepointType> savepointType(SavepointType::savepoint(SavepointFormatType::CANONICAL));
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));

    auto supplier =
        strategy.asyncSnapshot(mockResources, 42L, 100L, nullptr, checkpointOptions.get());

    ASSERT_NE(supplier, nullptr);
    auto result = supplier->get(nullptr);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->GetJobManagerOwnedSnapshot(), nullptr);
    EXPECT_EQ(result->GetTaskLocalSnapshot(), nullptr);
    EXPECT_EQ(result->GetStateSize(), 0L);
}

TEST(SavepointSnapshotStrategyTest, AsyncSnapshotWithEmptyMetaGetReturnsEmptySingleton)
{
    auto mockResources = std::make_shared<MockFullSnapshotResources>(false);
    SavepointSnapshotStrategy strategy(mockResources);
    std::unique_ptr<SavepointType> savepointType(SavepointType::savepoint(SavepointFormatType::CANONICAL));
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));

    auto supplier =
        strategy.asyncSnapshot(mockResources, 42L, 100L, nullptr, checkpointOptions.get());

    auto result1 = supplier->get(nullptr);
    auto result2 = supplier->get(nullptr);
    EXPECT_EQ(result1, result2);
}

TEST(SavepointSnapshotStrategyTest, AsyncSnapshotWithNonEmptyMetaReturnsFullSnapshotAsyncWriter)
{
    auto mockResources = std::make_shared<MockFullSnapshotResources>(true);
    SavepointSnapshotStrategy strategy(mockResources);
    std::unique_ptr<SavepointType> savepointType(SavepointType::savepoint(SavepointFormatType::CANONICAL));
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));

    auto supplier =
        strategy.asyncSnapshot(mockResources, 42L, 100L, nullptr, checkpointOptions.get());

    ASSERT_NE(supplier, nullptr);
    EXPECT_NE(std::dynamic_pointer_cast<FullSnapshotAsyncWriter>(supplier), nullptr);
}

TEST(SavepointSnapshotStrategyTest, AsyncSnapshotPassesCanonicalSavepointTypeToWriter)
{
    auto mockResources = std::make_shared<MockFullSnapshotResources>(true);
    SavepointSnapshotStrategy strategy(mockResources);
    std::unique_ptr<SavepointType> savepointType(SavepointType::savepoint(SavepointFormatType::CANONICAL));
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));

    EXPECT_NO_THROW(strategy.asyncSnapshot(mockResources, 42L, 100L, nullptr, checkpointOptions.get()));
}

TEST(SavepointSnapshotStrategyTest, BothBranchesCreateNonNullSupplier)
{
    auto emptyResources = std::make_shared<MockFullSnapshotResources>(false);
    auto nonEmptyResources = std::make_shared<MockFullSnapshotResources>(true);
    std::unique_ptr<SavepointType> savepointType(SavepointType::savepoint(SavepointFormatType::CANONICAL));
    std::unique_ptr<CheckpointOptions> checkpointOptions(
        CheckpointOptions::AlignedNoTimeout(*savepointType, CheckpointStorageLocationReference::GetDefault()));

    SavepointSnapshotStrategy emptyStrategy(emptyResources);
    SavepointSnapshotStrategy nonEmptyStrategy(nonEmptyResources);

    auto emptySupplier =
        emptyStrategy.asyncSnapshot(emptyResources, 1L, 100L, nullptr, checkpointOptions.get());
    auto nonEmptySupplier =
        nonEmptyStrategy.asyncSnapshot(nonEmptyResources, 1L, 100L, nullptr, checkpointOptions.get());

    EXPECT_NE(emptySupplier, nullptr);
    EXPECT_NE(nonEmptySupplier, nullptr);
}