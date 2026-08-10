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
#include <string>
#include <vector>

#include "runtime/state/StateRestoreValidation.h"
#include "runtime/state/memory/ByteStreamStateHandle.h"
#include "runtime/state/KeyGroupRange.h"
#include "runtime/state/KeyGroupRangeOffsets.h"
#include "runtime/state/KeyGroupsStateHandle.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "test/runtime/state/MockSavepointBridge.h"

using ::testing::_;
using ::testing::Return;

namespace {

StateMetaInfoSnapshot makeMetaInfo(const std::string& name)
{
    return StateMetaInfoSnapshot(
        name,
        StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

StateMetaInfoSnapshot makeNonKeyMetaInfo(const std::string& name, StateMetaInfoSnapshot::BackendStateType type)
{
    return StateMetaInfoSnapshot(
        name,
        type,
        std::unordered_map<std::string, std::string>{},
        std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

std::shared_ptr<KeyedStateHandle> makeKeyedStateHandle()
{
    KeyGroupRange keyGroupRange(0, 0);
    KeyGroupRangeOffsets offsets(keyGroupRange);
    auto streamHandle = std::make_shared<ByteStreamStateHandle>("state-restore-validation", std::vector<uint8_t>{1});
    return std::make_shared<KeyGroupsStateHandle>(offsets, streamHandle);
}

} // namespace

TEST(StateRestoreValidationTest, EmptyHandlesReturnsTrue)
{
    std::vector<std::shared_ptr<KeyedStateHandle>> handles;
    auto bridge = std::make_shared<MockSavepointBridge>();

    EXPECT_TRUE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, AllNonVbNamesHaveMatchingVb)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("state1"),
        makeMetaInfo("state1vb"),
        makeMetaInfo("state2"),
        makeMetaInfo("state2vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_TRUE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, MissingVbReturnsFalse)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("state1"),
        makeMetaInfo("state2vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, OnlyVbNamesReturnsFalse)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("state1vb"),
        makeMetaInfo("state2vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, SingleNonVbWithoutVbReturnsFalse)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("state1"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, MultipleHandlesAllValid)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle1 = makeKeyedStateHandle();
    auto handle2 = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle1, handle2};

    std::vector<StateMetaInfoSnapshot> metaInfos1 = {
        makeMetaInfo("state1"),
        makeMetaInfo("state1vb"),
    };
    std::vector<StateMetaInfoSnapshot> metaInfos2 = {
        makeMetaInfo("state2"),
        makeMetaInfo("state2vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos1)).WillOnce(Return(metaInfos2));

    EXPECT_TRUE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, MultipleHandlesSecondInvalid)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle1 = makeKeyedStateHandle();
    auto handle2 = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle1, handle2};

    std::vector<StateMetaInfoSnapshot> metaInfos1 = {
        makeMetaInfo("state1"),
        makeMetaInfo("state1vb"),
    };
    std::vector<StateMetaInfoSnapshot> metaInfos2 = {
        makeMetaInfo("state2"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos1)).WillOnce(Return(metaInfos2));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, ShortNameNotEndingWithVb)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("a"),
        makeMetaInfo("avb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_TRUE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, NameEndingWithVbButNotSuffix)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, VbSuffixCaseSensitive)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("state1"),
        makeMetaInfo("state1VB"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, ExtraVbWithoutNonVbReturnsFalse)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("state1"),
        makeMetaInfo("state1vb"),
        makeMetaInfo("state2vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, ExtraNonVbWithoutVbReturnsFalse)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeMetaInfo("state1"),
        makeMetaInfo("state1vb"),
        makeMetaInfo("state2"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, NonKeyValueEntriesAreSkipped)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeNonKeyMetaInfo("operator1", StateMetaInfoSnapshot::BackendStateType::OPERATOR),
        makeNonKeyMetaInfo("broadcast1", StateMetaInfoSnapshot::BackendStateType::BROADCAST),
        makeMetaInfo("state1"),
        makeMetaInfo("state1vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_TRUE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, OnlyNonKeyValueEntriesReturnsTrue)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeNonKeyMetaInfo("operator1", StateMetaInfoSnapshot::BackendStateType::OPERATOR),
        makeNonKeyMetaInfo("broadcast1", StateMetaInfoSnapshot::BackendStateType::BROADCAST),
        makeNonKeyMetaInfo("priority1", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_TRUE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, MixedKeyValueAndNonKeyValue)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeNonKeyMetaInfo("operator1", StateMetaInfoSnapshot::BackendStateType::OPERATOR),
        makeMetaInfo("state1"),
        makeMetaInfo("state1vb"),
        makeNonKeyMetaInfo("broadcast1", StateMetaInfoSnapshot::BackendStateType::BROADCAST),
        makeMetaInfo("state2"),
        makeMetaInfo("state2vb"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_TRUE(omnistream::validateRestoreStateHandles(handles, bridge));
}

TEST(StateRestoreValidationTest, NonKeyValueWithInvalidKeyValue)
{
    auto bridge = std::make_shared<MockSavepointBridge>();
    auto handle = makeKeyedStateHandle();
    std::vector<std::shared_ptr<KeyedStateHandle>> handles = {handle};

    std::vector<StateMetaInfoSnapshot> metaInfos = {
        makeNonKeyMetaInfo("operator1", StateMetaInfoSnapshot::BackendStateType::OPERATOR),
        makeMetaInfo("state1"),
    };
    EXPECT_CALL(*bridge, readMetaData(_)).WillOnce(Return(metaInfos));

    EXPECT_FALSE(omnistream::validateRestoreStateHandles(handles, bridge));
}
