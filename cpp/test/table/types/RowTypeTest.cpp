#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "table/types/logical/RowType.h"

using omniruntime::type::DataTypeId;
using omnistream::RowField;
using omnistream::RowType;

TEST(RowFieldTest, ConstructorsGettersAndJson)
{
    BasicLogicalType integerType(false, DataTypeId::OMNI_INT, "INTEGER");
    RowField describedField("id", &integerType, "primary key");

    EXPECT_EQ(describedField.getName(), "id");
    EXPECT_EQ(describedField.getType(), &integerType);
    EXPECT_EQ(
        describedField.toJson(),
        nlohmann::json(
            {{"name", "id"},
             {"fieldType", {{"nullable", false}, {"type", "INTEGER"}}},
             {"description", "primary key"}}));

    RowField fieldWithoutDescription("value", &integerType);
    EXPECT_EQ(fieldWithoutDescription.getName(), "value");
    EXPECT_EQ(fieldWithoutDescription.getType(), &integerType);
    EXPECT_EQ(fieldWithoutDescription.toJson()["description"], "");
}

TEST(RowTypeTest, FieldsConstructorSerializesAndCachesChildren)
{
    BasicLogicalType integerType(false, DataTypeId::OMNI_INT, "INTEGER");
    BasicLogicalType booleanType(true, DataTypeId::OMNI_BOOLEAN, "BOOLEAN");
    std::vector<RowField> fields{RowField("id", &integerType, "identifier"), RowField("enabled", &booleanType)};
    RowType rowType(false, fields);

    EXPECT_FALSE(rowType.isNullable());
    EXPECT_EQ(rowType.getTypeId(), DataTypeId::OMNI_CONTAINER);
    EXPECT_EQ(rowType.getTypeName(), "ROW");

    const auto children = rowType.getChildren();
    ASSERT_EQ(children.size(), 2);
    EXPECT_EQ(children[0], &integerType);
    EXPECT_EQ(children[1], &booleanType);

    const auto cachedChildren = rowType.getChildren();
    EXPECT_EQ(cachedChildren, children);

    EXPECT_EQ(
        rowType.toJson(),
        nlohmann::json(
            {{"nullable", false},
             {"type", "ROW"},
             {"fields",
              nlohmann::json::array(
                  {{{"name", "id"},
                    {"fieldType", {{"nullable", false}, {"type", "INTEGER"}}},
                    {"description", "identifier"}},
                   {{"name", "enabled"},
                    {"fieldType", {{"nullable", true}, {"type", "BOOLEAN"}}},
                    {"description", ""}}})}}));
}

TEST(RowTypeTest, TypeNamesConstructorCreatesIndexedFields)
{
    RowType rowType(true, std::vector<std::string>{"BIGINT", "BOOLEAN"});

    const auto children = rowType.getChildren();
    ASSERT_EQ(children.size(), 2);
    EXPECT_EQ(children[0]->getTypeId(), DataTypeId::OMNI_LONG);
    EXPECT_EQ(children[1]->getTypeId(), DataTypeId::OMNI_BOOLEAN);

    EXPECT_EQ(
        rowType.toJson(),
        nlohmann::json(
            {{"nullable", true},
             {"type", "ROW"},
             {"fields",
              nlohmann::json::array(
                  {{{"name", "f0"}, {"fieldType", {{"nullable", true}, {"type", "BIGINT"}}}, {"description", ""}},
                   {{"name", "f1"},
                    {"fieldType", {{"nullable", true}, {"type", "BOOLEAN"}}},
                    {"description", ""}}})}}));
}

TEST(RowTypeTest, EmptyFieldsRemainEmpty)
{
    RowType rowType(true, std::vector<RowField>{});

    EXPECT_TRUE(rowType.getChildren().empty());
    EXPECT_EQ(rowType.toJson()["fields"], nlohmann::json::array());
}
