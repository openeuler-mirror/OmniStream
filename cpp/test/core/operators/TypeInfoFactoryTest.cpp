#include <gtest/gtest.h>
#include "typeinfo/TypeInfoFactory.h"
#include "table/types/logical/LogicalType.h"
#include "typeinfo/StringTypeInfo.h"
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <iostream>

// 测试用例定义
TEST(TypeInfoFactoryTest, CreateTypeInfoStringTest) {
    TypeInformation* typeInfo = TypeInfoFactory::createTypeInfo(TYPE_NAME_STRING, "");
    EXPECT_TRUE(typeInfo != nullptr);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, CreateTypeInfoUnsupportedTypeTest) {
    EXPECT_THROW({
                     try {
                         TypeInformation* typeInfo = TypeInfoFactory::createTypeInfo("UnsupportedType", "");
                         delete typeInfo;
                     } catch (const std::exception& e) {
                         THROW_LOGIC_EXCEPTION("Exception caught: " + std::string(e.what()));
                     }
                 }, std::exception);
}

TEST(TypeInfoFactoryTest, CreateInternalTypeInfoRowTest) {
    const json &rowType = R"(
            [{"type":"Long"}, {"type":"Long"}]
    )"_json;

    TypeInformation* typeInfo = TypeInfoFactory::createTupleTypeInfo(rowType);
    EXPECT_TRUE(typeInfo != nullptr);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, CreateTupleTypeInfoNonArrayTest) {
    json fieldType = {
            {"type", "BIGINT"}
    };

    EXPECT_THROW({
                     try {
                         TypeInformation* typeInfo = TypeInfoFactory::createTupleTypeInfo(fieldType);
                         delete typeInfo;
                     } catch (const std::exception& e) {
                         THROW_LOGIC_EXCEPTION("Exception caught: " + std::string(e.what()));
                     }
                 }, std::exception);
}

TEST(TypeInfoFactoryTest, CreateCommittableMessageInfoTest) {
    TypeInformation* typeInfo = TypeInfoFactory::createCommittableMessageInfo();
    EXPECT_TRUE(typeInfo != nullptr);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, CreateBasicInternalTypeInfoTest) {
    EXPECT_THROW({
                     try {
                         TypeInformation* typeInfo = TypeInfoFactory::createBasicInternalTypeInfo("UnsupportedType", "");
                         delete typeInfo;
                     } catch (const std::exception& e) {
                         THROW_LOGIC_EXCEPTION("Exception caught: " + std::string(e.what()));
                     }
                 }, std::exception);
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeBigIntTest) {
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'BIGINT'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::BIGINT->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimeWithoutTimeZoneTest) {
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIME_WITHOUT_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIME_WITHOUT_TIME_ZONE->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimestampWithoutTimeZoneTest) {
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIMESTAMP_WITHOUT_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIMESTAMP_WITHOUT_TIME_ZONE->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimestampWithTimeZoneTest) {
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIMESTAMP_WITH_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIMESTAMP_WITH_TIME_ZONE->getTypeId());
    delete logicalType;
}

TEST(TypeInfoFactoryTest, CreateLogicalTypeTimestampWithLocalTimeZoneTest) {
    LogicalType* logicalType = TypeInfoFactory::createLogicalType("'TIMESTAMP_WITH_LOCAL_TIME_ZONE'");
    EXPECT_TRUE(logicalType->getTypeId() == BasicLogicalType::TIMESTAMP_WITH_LOCAL_TIME_ZONE->getTypeId());
    delete logicalType;
}

