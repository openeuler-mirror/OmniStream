#include <gtest/gtest.h>
#include "table/runtime/operators/aggregate/window/RecordCounter.h"
#include "table/data/utils/JoinedRowData.h"

TEST(RecordCounterTest, AccumulationRecordCounterTest) {
    JoinedRowData joinedRowData(nullptr, nullptr);
    auto accumulationRecordCounter = RecordCounter::of(-1);
    
    EXPECT_TRUE(accumulationRecordCounter->recordCountIsZero(nullptr));
    EXPECT_FALSE(accumulationRecordCounter->recordCountIsZero(&joinedRowData));

}

TEST(RecordCounterTest, RetractionRecordCounterTest) {
    JoinedRowData joinedRowData(nullptr, nullptr);
    auto retractionRecordCounter = RecordCounter::of(0);
        
    EXPECT_TRUE(retractionRecordCounter->recordCountIsZero(nullptr));
    // EXPECT_FALSE(retractionRecordCounter->recordCountIsZero(&joinedRowData));

}
