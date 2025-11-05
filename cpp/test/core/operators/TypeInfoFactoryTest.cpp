#include <gtest/gtest.h>
#include "typeinfo/TypeInfoFactory.h"
#include "table/types/logical/LogicalType.h"
#include "typeinfo/StringTypeInfo.h"
#include "execution/Environment.h"
#include "streaming/api/operators/StreamTaskStateInitializerImpl.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "streaming/api/operators/StreamOperatorStateHandler.h"
#include "typeinfo/LongTypeInfo.h"
#include "test/core/operators/source/User.h"
#include "basictypes/ReflectMacros.h"
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <iostream>

// 测试用例定义
TEST(TypeInfoFactoryTest, DISABLED_PojoValueStateTest) {
    std::string pojo = "{\n"
                       "\t\"fieldSerializers\": [\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.LongSerializer\"\n"
                       "\t\t},\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.StringSerializer\"\n"
                       "\t\t}\n"
                       "\t],\n"
                       "\t\"serializerName\": \"org.apache.flink.api.java.typeutils.runtime.PojoSerializer\",\n"
                       "\t\"fields\": [\n"
                       "\t\t\"age\",\n"
                       "\t\t\"name\"\n"
                       "\t],\n"
                       "\t\"clazz\": \"User\"\n"
                       "}";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(pojo);
    TypeInformation* typeInfo = TypeInfoFactory::createDataStreamTypeInfo(opDescriptionJSON);
    omnistream::RuntimeEnvironmentV2 *env = new omnistream::RuntimeEnvironmentV2();
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<long> *context = initializer.streamOperatorStateContext<long>(new LongSerializer(), nullptr, nullptr);
    auto stateHandler = new StreamOperatorStateHandler<long>(context);
    auto keyStore = stateHandler->getKeyedStateStore();
    auto vs = new DataStreamValueStateDescriptor("vs", typeInfo);
    vs->SetStateSerializer(typeInfo->createTypeSerializer());
    auto stateVs = keyStore->getState(vs);
    auto value = stateVs->value();
    ASSERT_EQ(value, nullptr);
    auto user = new User();
    user->name = new String("tiantao");
    user->age = 30L;
    stateVs->update(user);
    auto stateUser = reinterpret_cast<User*>(stateVs->value());
    ASSERT_EQ(stateUser->name->getValue(), "tiantao");
    ASSERT_EQ(stateUser->age, 30L);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, DISABLED_PojoMapStateTest) {
    std::string pojo = "{\n"
                       "\t\"fieldSerializers\": [\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.LongSerializer\"\n"
                       "\t\t},\n"
                       "\t\t{\n"
                       "\t\t\t\"serializerName\": \"org.apache.flink.api.common.typeutils.base.StringSerializer\"\n"
                       "\t\t}\n"
                       "\t],\n"
                       "\t\"serializerName\": \"org.apache.flink.api.java.typeutils.runtime.PojoSerializer\",\n"
                       "\t\"fields\": [\n"
                       "\t\t\"age\",\n"
                       "\t\t\"name\"\n"
                       "\t],\n"
                       "\t\"clazz\": \"User\"\n"
                       "}";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(pojo);
    TypeInformation* typeInfo = TypeInfoFactory::createDataStreamTypeInfo(opDescriptionJSON);
    omnistream::RuntimeEnvironmentV2 *env = new omnistream::RuntimeEnvironmentV2();
    StreamTaskStateInitializerImpl initializer(env);
    StreamOperatorStateContextImpl<long> *context = initializer.streamOperatorStateContext<long>(new LongSerializer(), nullptr, nullptr);
    auto stateHandler = new StreamOperatorStateHandler<long>(context);
    auto keyStore = stateHandler->getKeyedStateStore();
    auto keyInfo = new LongTypeInfo("long");

    auto ms = new DataStreamMapStateDescriptor("ms", new BasicTypeInfo(LongSerializer::INSTANCE, TYPE_NAME_LONG_SERIALIZER) ,typeInfo);
    auto stateMs = keyStore->getMapState(ms);
    auto iterator = stateMs->iterator();
    bool hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, false);

    auto user = new User();
    user->name = new String("tiantao");
    user->age = 30L;
    auto givenKey = new Long(50);
    stateMs->put(givenKey, user);
    auto valueUser = reinterpret_cast<User*>(stateMs->get(givenKey).value());
    ASSERT_EQ(valueUser->name->getValue(), "tiantao");
    ASSERT_EQ(valueUser->age, 30L);

    iterator = stateMs->iterator();
    hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, true);
    auto entry = reinterpret_cast<MapEntry*>(iterator->next());
    valueUser = reinterpret_cast<User*>(entry->getValue());
    ASSERT_EQ(valueUser->name->getValue(), "tiantao");
    ASSERT_EQ(valueUser->age, 30L);

    auto key = reinterpret_cast<Long*>(entry->getKey());
    ASSERT_EQ(key->getValue(), 50L);

    hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, false);

    bool contains = stateMs->contains(givenKey);
    ASSERT_EQ(contains, true);

    stateMs->remove(givenKey);
    contains = stateMs->contains(givenKey);
    ASSERT_EQ(contains, false);
    ASSERT_EQ(stateMs->get(givenKey).has_value(), false);

    iterator = stateMs->iterator();
    hasNext = iterator->hasNext();
    ASSERT_EQ(hasNext, false);
}

// 测试用例定义
TEST(TypeInfoFactoryTest, CreateTypeInfoStringTest) {
    TypeInformation* typeInfo = TypeInfoFactory::createTypeInfo(TYPE_NAME_STRING);
    EXPECT_TRUE(typeInfo != nullptr);
    delete typeInfo;
}

TEST(TypeInfoFactoryTest, CreateTypeInfoUnsupportedTypeTest) {
    EXPECT_THROW({
                     try {
                         TypeInformation* typeInfo = TypeInfoFactory::createTypeInfo("UnsupportedType");
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
                         TypeInformation* typeInfo = TypeInfoFactory::createBasicInternalTypeInfo("UnsupportedType");
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

