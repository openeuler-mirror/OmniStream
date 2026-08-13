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

#include <gtest/gtest.h>

#include <nlohmann/json.hpp>

#include "../../../runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptorFactory.h"
#include "runtime/checkpoint/OperatorSavepointAdaptor.h"
#include "runtime/checkpoint/StreamingJoinSavepointUtil.h"
#include "runtime/checkpoint/WindowJoinSavepointAdaptor.h"
#include "runtime/checkpoint/GroupWindowAggSavepointAdaptor.h"
#include "runtime/checkpoint/WindowAggSavepointCompatibility.h"

using omnistream::OperatorSavepointAdaptorFactory;
using omnistream::StreamingJoinSavepointUtil;
using omnistream::WindowAggSavepointCompatibility;

namespace {
nlohmann::json createStreamingJoinDescription(const std::string& timestampType)
{
    return {
        {"joinType", "InnerJoin"},
        {"leftInputSpec", "NoUniqueKey"},
        {"rightInputSpec", "NoUniqueKey"},
        {"leftUniqueKeys", nlohmann::json::array()},
        {"rightUniqueKeys", nlohmann::json::array()},
        {"leftInputTypes", {"BIGINT", timestampType}},
        {"rightInputTypes", {"BIGINT"}},
        {"leftJoinKey", {0}},
        {"rightJoinKey", {0}},
        {"filterNulls", {true}},
    };
}
} // namespace

// None：不支持兼容 savepoint，工厂返回 nullptr。
TEST(OperatorSavepointAdaptorFactoryTest, ReturnsNullForNone)
{
    EXPECT_EQ(OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::None), nullptr);
}

// OmniIsCompatible：走 canonical 路径，无需 Adaptor，工厂返回 nullptr。
TEST(OperatorSavepointAdaptorFactoryTest, ReturnsNullForOmniIsCompatible)
{
    EXPECT_EQ(OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible), nullptr);
}

// DeduplicateAdaptor：已实现的 Adaptor，工厂返回 DeduplicateSavepointAdaptor 实例。
TEST(OperatorSavepointAdaptorFactoryTest, ReturnsDeduplicateAdaptor)
{
    auto adaptor = OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::DeduplicateAdaptor);
    EXPECT_NE(adaptor, nullptr);
}

// AppendOnlyTopNAdaptor：已实现的 Adaptor，工厂返回 AppendOnlyTopNSavepointAdaptor 实例。
TEST(OperatorSavepointAdaptorFactoryTest, ReturnsAppendOnlyTopNAdaptor)
{
    auto adaptor = OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::AppendOnlyTopNAdaptor);
    EXPECT_NE(adaptor, nullptr);
}

TEST(OperatorSavepointAdaptorFactoryTest, ReturnsWindowJoinAdaptor)
{
    auto adaptor = OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::WindowJoinAdaptor);
    EXPECT_NE(dynamic_cast<omnistream::WindowJoinSavepointAdaptor*>(adaptor.get()), nullptr);
}

TEST(OperatorSavepointAdaptorFactoryTest, ReturnsGroupWindowAggAdaptor)
{
    auto adaptor = OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::GroupWindowAggAdaptor);
    EXPECT_NE(dynamic_cast<omnistream::GroupWindowAggSavepointAdaptor*>(adaptor.get()), nullptr);

    const nlohmann::json sessionWindowDescription = {
        {"windowType", "SessionGroupWindow('w$, dateTime, 10000)"},
    };
    // window-aggs is byte-compatible, so adaptor preparation must not depend
    // on accumulator schema or construct an accumulator serializer.
    EXPECT_NO_THROW(adaptor->prepareForSave(sessionWindowDescription));
    EXPECT_NO_THROW(adaptor->prepareForRestore(sessionWindowDescription));
}
// StreamingJoinAdaptor：已实现的 NoUniqueKey inner/left outer join 互通 Adaptor，
TEST(OperatorSavepointAdaptorFactoryTest, ReturnsStreamingJoinAdaptors)
{
    EXPECT_NE(
        OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor),
        nullptr);
    EXPECT_NE(
        OperatorSavepointAdaptorFactory::createAdaptor(
            FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor),
        nullptr);
}

// 预留但尚未实现的 Adaptor 类型，工厂目前统一返回 nullptr。
TEST(OperatorSavepointAdaptorFactoryTest, ReturnsNullForNotYetImplementedTypes)
{
    EXPECT_EQ(OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::OmniIsCompatible), nullptr);
    EXPECT_EQ(OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::None), nullptr);
}

TEST(OperatorSavepointAdaptorFactoryTest, GroupWindowAggAdaptorRecognizesStructuredWindowType)
{
    auto adaptor = OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::GroupWindowAggAdaptor);
    ASSERT_NE(adaptor, nullptr);

    EXPECT_NO_THROW(adaptor->prepareForSave({{"windowKind", "SESSION"}}));
    EXPECT_NO_THROW(adaptor->prepareForRestore({{"windowTypeName", "TumblingGroupWindow"}}));
    EXPECT_NO_THROW(adaptor->prepareForSave({{"windowType", {{"kind", "SESSION"}}}}));
}

TEST(OperatorSavepointAdaptorFactoryTest, GroupWindowAggAdaptorRejectsUnknownWindowType)
{
    auto adaptor = OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::GroupWindowAggAdaptor);
    ASSERT_NE(adaptor, nullptr);

    EXPECT_THROW(adaptor->prepareForSave(nlohmann::json::object()), std::runtime_error);
    EXPECT_THROW(adaptor->prepareForRestore({{"windowType", "UnknownGroupWindow()"}}), std::runtime_error);
}

TEST(WindowAggSavepointCompatibilityTest, LocalWindowAggIsCompatible)
{
    const auto info = WindowAggSavepointCompatibility::forLocal();

    EXPECT_EQ(info.type, FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_TRUE(info.reason.empty());
}

TEST(WindowAggSavepointCompatibilityTest, SelectsSlicingAccumulatorTypesFromOperatorMode)
{
    const nlohmann::json localDescription = {
        {"isWindowAggregate", true},
        {"aggInfoList", {{"AccTypes", {"BIGINT"}}, {"globalAccTypes", {"RAW(unused)"}}}},
    };
    const nlohmann::json globalDescription = {
        {"isWindowAggregate", false},
        {"aggInfoList", {{"AccTypes", {"RAW(unused)"}}, {"globalAccTypes", {"BIGINT"}}}},
    };

    EXPECT_EQ(
        WindowAggSavepointCompatibility::forSlicing(localDescription).type,
        FlinkSavepointAdaptorType::OmniIsCompatible);
    EXPECT_EQ(
        WindowAggSavepointCompatibility::forSlicing(globalDescription).type,
        FlinkSavepointAdaptorType::OmniIsCompatible);
}

TEST(WindowAggSavepointCompatibilityTest, RejectsOnlyActualRawAccumulatorTypes)
{
    const nlohmann::json rawDescription = {
        {"aggInfoList", {{"AccTypes", {"RAW(org.example.DataView)"}}}},
    };
    const nlohmann::json nonRawDescription = {
        {"aggInfoList", {{"AccTypes", {"DRAWING"}}}},
    };

    EXPECT_EQ(WindowAggSavepointCompatibility::forGroup(rawDescription).type, FlinkSavepointAdaptorType::None);
    EXPECT_EQ(
        WindowAggSavepointCompatibility::forGroup(nonRawDescription).type,
        FlinkSavepointAdaptorType::GroupWindowAggAdaptor);
}

TEST(WindowAggSavepointCompatibilityTest, RejectsMalformedAccumulatorTypesWithoutThrowing)
{
    const nlohmann::json description = {
        {"aggInfoList", {{"AccTypes", {nlohmann::json::object()}}}},
    };

    FlinkSavepointAdaptorInfo info;
    EXPECT_NO_THROW(info = WindowAggSavepointCompatibility::forGroup(description));
    EXPECT_EQ(info.type, FlinkSavepointAdaptorType::None);
}

TEST(StreamingJoinSavepointUtilTest, OnlySupportsCompactTimestampPrecision)
{
    auto description = createStreamingJoinDescription("TIMESTAMP(3)");
    for (const std::string& timestampType : {"TIMESTAMP(3)", "TIMESTAMP_LTZ(3)"}) {
        description["leftInputTypes"][1] = timestampType;
        EXPECT_EQ(
            StreamingJoinSavepointUtil::getAdaptorType(description),
            FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor);
    }

    for (const std::string& timestampType : {"TIMESTAMP(6)", "TIMESTAMP(9)", "TIMESTAMP_LTZ(6)"}) {
        description["leftInputTypes"][1] = timestampType;
        EXPECT_EQ(StreamingJoinSavepointUtil::getAdaptorType(description), FlinkSavepointAdaptorType::None);
    }
    EXPECT_EQ(
        StreamingJoinSavepointUtil::buildUnsupportedReason(description),
        "StreamingJoin compatible savepoint only supports BIGINT, VARCHAR/STRING and TIMESTAMP input fields with "
        "precision <= 3");
}

TEST(StreamingJoinSavepointUtilTest, MatchesOnlySupportedNoUniqueKeyJoinContract)
{
    auto description = createStreamingJoinDescription("TIMESTAMP(3)");
    description["joinType"] = "LeftOuterJoin";
    EXPECT_EQ(
        StreamingJoinSavepointUtil::getAdaptorType(description),
        FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor);

    description = createStreamingJoinDescription("TIMESTAMP(3)");
    description["leftUniqueKeys"] = nlohmann::json::array({nlohmann::json::array({0})});
    EXPECT_EQ(StreamingJoinSavepointUtil::getAdaptorType(description), FlinkSavepointAdaptorType::None);
    EXPECT_EQ(
        StreamingJoinSavepointUtil::buildUnsupportedReason(description),
        "StreamingJoin compatible savepoint does not support unique-key join state");

    description = createStreamingJoinDescription("TIMESTAMP(3)");
    description["leftJoinKey"] = {1};
    EXPECT_EQ(StreamingJoinSavepointUtil::getAdaptorType(description), FlinkSavepointAdaptorType::None);
    EXPECT_EQ(
        StreamingJoinSavepointUtil::buildUnsupportedReason(description),
        "StreamingJoin compatible savepoint requires BIGINT join keys");

    description = createStreamingJoinDescription("TIMESTAMP(3)");
    description["leftStateName"] = "unexpected-state";
    EXPECT_EQ(StreamingJoinSavepointUtil::getAdaptorType(description), FlinkSavepointAdaptorType::None);
    EXPECT_EQ(
        StreamingJoinSavepointUtil::buildUnsupportedReason(description),
        "StreamingJoin compatible savepoint state names do not match left-records/right-records");

    description = createStreamingJoinDescription("TIMESTAMP(3)");
    description.erase("filterNulls");
    EXPECT_EQ(StreamingJoinSavepointUtil::getAdaptorType(description), FlinkSavepointAdaptorType::None);
    EXPECT_EQ(
        StreamingJoinSavepointUtil::buildUnsupportedReason(description),
        "StreamingJoin compatible savepoint requires filterNulls metadata");
}

// FlinkSavepointAdaptorInfo 默认值：type 为 None，reason 为空。
TEST(FlinkSavepointAdaptorInfoTest, DefaultConstructedIsNone)
{
    FlinkSavepointAdaptorInfo info;
    EXPECT_EQ(info.type, FlinkSavepointAdaptorType::None);
    EXPECT_TRUE(info.reason.empty());
}
