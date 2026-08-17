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

#include "table/types/logical/LogicalType.h"
#include "typeinfo/ExternalTypeInfo.h"
#include "typeutils/ExternalSerializer.h"
#include "typeutils/LongSerializer.h"

namespace {

std::shared_ptr<LogicalType> shareLogicalType(LogicalType* logicalType)
{
    return std::shared_ptr<LogicalType>(logicalType, [](LogicalType*) {});
}

class TrackingTypeInfo final : public TypeInformation {
public:
    explicit TrackingTypeInfo(bool& destroyed) : destroyed_(destroyed)
    {
    }

    ~TrackingTypeInfo() override
    {
        destroyed_ = true;
    }

    TypeSerializer* createTypeSerializer() override
    {
        return new LongSerializer();
    }

    BackendDataType getBackendId() const override
    {
        return BackendDataType::BIGINT_BK;
    }

    std::string name() override
    {
        return "TrackingTypeInfo";
    }

private:
    bool& destroyed_;
};

TEST(ExternalTypeInfoTest, ExposesConfigurationAndCreatesExternalSerializer)
{
    bool internalTypeInfoDestroyed = false;
    auto dataType = shareLogicalType(BasicLogicalType::BIGINT);

    {
        ExternalTypeInfo typeInfo(dataType, new TrackingTypeInfo(internalTypeInfoDestroyed), true, "java.lang.Long");

        EXPECT_EQ(typeInfo.getDataType(), BasicLogicalType::BIGINT);
        EXPECT_NE(typeInfo.getInternalTypeInfo(), nullptr);
        EXPECT_TRUE(typeInfo.isInternalInput());
        EXPECT_EQ(typeInfo.getConversionClass(), "java.lang.Long");
        EXPECT_EQ(typeInfo.name(), "ExternalTypeInfo<java.lang.Long>");
        EXPECT_EQ(typeInfo.getBackendId(), BackendDataType::EXTERNAL_BIGINT_BK);

        std::unique_ptr<TypeSerializer> serializer(typeInfo.createTypeSerializer());
        auto* externalSerializer = dynamic_cast<ExternalSerializer*>(serializer.get());
        ASSERT_NE(externalSerializer, nullptr);
        EXPECT_EQ(externalSerializer->getDataType(), BasicLogicalType::BIGINT);
        EXPECT_EQ(externalSerializer->getBackendId(), BackendDataType::EXTERNAL_BIGINT_BK);
        const auto serializerJson = nlohmann::json::parse(externalSerializer->toJson());
        EXPECT_TRUE(serializerJson["serializerAttributes"]["externalIsInternalInput"].get<bool>());
    }

    EXPECT_TRUE(internalTypeInfoDestroyed);
}

TEST(ExternalTypeInfoTest, ReturnsInvalidBackendForNonBigIntAndAcceptsNullInternalTypeInfo)
{
    ExternalTypeInfo typeInfo(shareLogicalType(BasicLogicalType::INTEGER), nullptr, false, "");

    EXPECT_EQ(typeInfo.getBackendId(), BackendDataType::INVALID_BK);
    EXPECT_EQ(typeInfo.getInternalTypeInfo(), nullptr);
    EXPECT_FALSE(typeInfo.isInternalInput());
    EXPECT_EQ(typeInfo.getConversionClass(), "");
    EXPECT_EQ(typeInfo.name(), "ExternalTypeInfo<>");
}

} // namespace
