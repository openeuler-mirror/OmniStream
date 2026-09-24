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

#include <string>

#include <nlohmann/json.hpp>

#include "runtime/checkpoint/WindowAggSavepointCompatibility.h"

namespace {

nlohmann::json makeValidDescription(const std::string& fieldName,
    const std::vector<std::string>& accumulators)
{
    nlohmann::json accTypes = nlohmann::json::array();
    for (const auto& acc : accumulators) {
        accTypes.push_back(acc);
    }
    nlohmann::json aggInfoList;
    aggInfoList[fieldName] = accTypes;
    nlohmann::json desc;
    desc["aggInfoList"] = aggInfoList;
    return desc;
}

} // namespace

// =========================================================================
// WindowAggSavepointCompatibility::forLocal()
// =========================================================================

TEST(WindowAggSavepointCompatibilityTest, ForLocalReturnsOmniIsCompatible)
{
    auto result = omnistream::WindowAggSavepointCompatibility::forLocal();
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(result.reason.empty());
}

// =========================================================================
// WindowAggSavepointCompatibility::forSlicing()
// =========================================================================

TEST(WindowAggSavepointCompatibilityTest, ForSlicingWithWindowAggregateUsesAccTypes)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {"VARCHAR", "BIGINT"});
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingWithNonWindowAggregateUsesGlobalAccTypes)
{
    nlohmann::json desc = makeValidDescription("globalAccTypes", {"VARCHAR", "BIGINT"});
    desc["isWindowAggregate"] = false;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingDefaultsToGlobalAccTypes)
{
    // isWindowAggregate not set at all → defaults to false → uses globalAccTypes.
    nlohmann::json desc = makeValidDescription("globalAccTypes", {"VARCHAR"});

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingMissingAggInfoList)
{
    nlohmann::json desc;
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
    EXPECT_NE(result.reason.find("aggInfoList"), std::string::npos);
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingAggInfoListNotObject)
{
    nlohmann::json desc;
    desc["aggInfoList"] = "not_an_object";
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingMissingField)
{
    // Uses AccTypes (isWindowAggregate=true) but AccTypes is not present.
    nlohmann::json desc;
    desc["aggInfoList"] = nlohmann::json::object();
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingFieldNotArray)
{
    nlohmann::json desc;
    desc["aggInfoList"]["AccTypes"] = "not_an_array";
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingNonStringAccumulatorType)
{
    nlohmann::json desc;
    desc["aggInfoList"]["AccTypes"] = nlohmann::json::array({123, 456});
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingRawAccumulator)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {"VARCHAR", "RAW"});
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
    EXPECT_NE(result.reason.find("RAW"), std::string::npos);
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingRawWithParenthesisAccumulator)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {"RAW(some,args)"});
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingEmptyAccumulatorList)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {});
    desc["isWindowAggregate"] = true;

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForSlicingMixedValidAndRaw)
{
    nlohmann::json desc = makeValidDescription("globalAccTypes", {"BIGINT", "RAW"});

    auto result = omnistream::WindowAggSavepointCompatibility::forSlicing(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

// =========================================================================
// WindowAggSavepointCompatibility::forGroup()
// =========================================================================

TEST(WindowAggSavepointCompatibilityTest, ForGroupCompatibleWithNonRawAccumulators)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {"VARCHAR", "BIGINT", "INT"});

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupMissingAggInfoList)
{
    nlohmann::json desc;

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
    EXPECT_NE(result.reason.find("aggInfoList"), std::string::npos);
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupAggInfoListNotObject)
{
    nlohmann::json desc;
    desc["aggInfoList"] = 42;

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupMissingAccTypes)
{
    nlohmann::json desc;
    desc["aggInfoList"] = nlohmann::json::object();

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupAccTypesNotArray)
{
    nlohmann::json desc;
    desc["aggInfoList"]["AccTypes"] = "not_an_array";

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupNonStringAccumulatorType)
{
    nlohmann::json desc;
    desc["aggInfoList"]["AccTypes"] = nlohmann::json::array({true, false});

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupRawAccumulator)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {"RAW"});

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
    EXPECT_NE(result.reason.find("RAW"), std::string::npos);
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupRawWithParenthesisAccumulator)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {"RAW(arg1,arg2)"});

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::None);
    EXPECT_FALSE(result.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, ForGroupEmptyAccumulatorList)
{
    nlohmann::json desc = makeValidDescription("AccTypes", {});

    auto result = omnistream::WindowAggSavepointCompatibility::forGroup(desc);
    EXPECT_EQ(result.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(result.reason.empty());
}