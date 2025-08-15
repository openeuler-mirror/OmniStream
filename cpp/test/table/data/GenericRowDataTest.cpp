#include "table/data/GenericRowData.h"
#include <gtest/gtest.h>


TEST(GenericRowDataTest, ConstructorTest_1) {
    GenericRowData genericRowData(3);
    EXPECT_EQ(genericRowData.getRowKind(), RowKind::INSERT);
    EXPECT_EQ(genericRowData.getArity(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs().size(),3);
}

TEST(GenericRowDataTest, ConstructorTest_2) {
    std::vector<int> typeIDs({0,1,2});
    GenericRowData genericRowData(typeIDs,RowKind::INSERT);
    EXPECT_EQ(genericRowData.getRowKind(), RowKind::INSERT);
    EXPECT_EQ(genericRowData.getArity(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs().size(),3);
    EXPECT_EQ(genericRowData.getTypeIDs(),typeIDs);
}

TEST(GenericRowDataTest, ConstructorTest_3) {
    std::vector<int> typeIDs({0,1,2});
    GenericRowData genericRowData(typeIDs);
    EXPECT_EQ(genericRowData.getRowKind(), RowKind::INSERT);
    EXPECT_EQ(genericRowData.getArity(), 3);
    EXPECT_EQ(genericRowData.getTypeIDs().size(),3);
    EXPECT_EQ(genericRowData.getTypeIDs(),typeIDs);
}

TEST(GenericRowDataTest, SetFieldTest) {
    std::vector<int> typeIDs({0,0});
    GenericRowData genericRowData(typeIDs);
    genericRowData.setField(0,1);
    genericRowData.setField(1,2);
    
    EXPECT_EQ(genericRowData.getField(0),1);
    EXPECT_EQ(genericRowData.getField(1),2);
}

TEST(GenericRowDataTest, TimeStampTest){
    std::vector<int> typeIDs({12});
    GenericRowData genericRowData(typeIDs);
    TimestampData *timeStampData = new TimestampData(1000, 1);
    genericRowData.setField(0, timeStampData);

    EXPECT_EQ(genericRowData.getTimestamp(0)->getMillisecond(), 1000);
}