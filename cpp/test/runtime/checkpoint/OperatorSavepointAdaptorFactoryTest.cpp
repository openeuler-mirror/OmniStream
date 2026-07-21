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

#include "runtime/checkpoint/FlinkSavepointAdaptorInfo.h"
#include "runtime/checkpoint/OperatorSavepointAdaptorFactory.h"
#include "runtime/checkpoint/OperatorSavepointAdaptor.h"

using omnistream::OperatorSavepointAdaptorFactory;

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

// 预留但尚未实现的 Adaptor 类型，工厂目前统一返回 nullptr。
TEST(OperatorSavepointAdaptorFactoryTest, ReturnsNullForNotYetImplementedTypes)
{
    EXPECT_EQ(
        OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::AppendOnlyTopNAdaptor), nullptr);
    EXPECT_EQ(
        OperatorSavepointAdaptorFactory::createAdaptor(FlinkSavepointAdaptorType::StreamingJoinNoUniqueKeyAdaptor),
        nullptr);
    EXPECT_EQ(
        OperatorSavepointAdaptorFactory::createAdaptor(
            FlinkSavepointAdaptorType::StreamingLeftOuterJoinNoUniqueKeyAdaptor),
        nullptr);
}

// FlinkSavepointAdaptorInfo 默认值：type 为 None，reason 为空。
TEST(FlinkSavepointAdaptorInfoTest, DefaultConstructedIsNone)
{
    FlinkSavepointAdaptorInfo info;
    EXPECT_EQ(info.type, FlinkSavepointAdaptorType::None);
    EXPECT_TRUE(info.reason.empty());
}
