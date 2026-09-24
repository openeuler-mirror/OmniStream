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

#include "runtime/state/FullSnapshotResources.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyValueStateIterator.h"
#include "runtime/state/SavepointResources.h"
#include "runtime/state/SnapshotExecutionType.h"
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

TEST(SavepointResourcesTest, ConstructorStoresSnapshotResources)
{
    auto resources = std::make_shared<MockFullSnapshotResources>(false);
    SavepointResources spResources(resources, SnapshotExecutionType::ASYNCHRONOUS);

    EXPECT_EQ(spResources.getSnapshotResources(), resources);
}

TEST(SavepointResourcesTest, ConstructorStoresPreferredExecutionType)
{
    auto resources = std::make_shared<MockFullSnapshotResources>(false);

    SavepointResources asyncResources(resources, SnapshotExecutionType::ASYNCHRONOUS);
    EXPECT_EQ(asyncResources.getPreferredSnapshotExecutionType(), SnapshotExecutionType::ASYNCHRONOUS);

    SavepointResources syncResources(resources, SnapshotExecutionType::SYNCHRONOUS);
    EXPECT_EQ(syncResources.getPreferredSnapshotExecutionType(), SnapshotExecutionType::SYNCHRONOUS);
}

TEST(SavepointResourcesTest, StoresNullResources)
{
    SavepointResources spResources(nullptr, SnapshotExecutionType::ASYNCHRONOUS);
    EXPECT_EQ(spResources.getSnapshotResources(), nullptr);
}

TEST(SavepointResourcesTest, ResourcesAreIndependentAcrossInstances)
{
    auto resources1 = std::make_shared<MockFullSnapshotResources>(false);
    auto resources2 = std::make_shared<MockFullSnapshotResources>(true);

    SavepointResources spRes1(resources1, SnapshotExecutionType::SYNCHRONOUS);
    SavepointResources spRes2(resources2, SnapshotExecutionType::ASYNCHRONOUS);

    EXPECT_NE(spRes1.getSnapshotResources(), spRes2.getSnapshotResources());
    EXPECT_EQ(spRes1.getPreferredSnapshotExecutionType(), SnapshotExecutionType::SYNCHRONOUS);
    EXPECT_EQ(spRes2.getPreferredSnapshotExecutionType(), SnapshotExecutionType::ASYNCHRONOUS);
}