#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "table/runtime/operators/window/assigners/SlidingWindowAssigner.h"
#include "table/runtime/operators/window/assigners/TumblingWindowAssigner.h"

namespace {

void AssertWindowsEq(const std::vector<TimeWindow>& actual, const std::vector<TimeWindow>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]) << "at index " << i;
    }
}

}

TEST(TumblingWindowAssignerTest, WindowAssignmentMatchesFlink)
{
    TumblingWindowAssigner assigner(5000, 0, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 0L), {TimeWindow(0, 5000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 4999L), {TimeWindow(0, 5000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5000L), {TimeWindow(5000, 10000)});
}

TEST(TumblingWindowAssignerTest, WindowAssignmentWithOffsetMatchesFlink)
{
    TumblingWindowAssigner assigner(5000, 100, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 100L), {TimeWindow(100, 5100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5099L), {TimeWindow(100, 5100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5100L), {TimeWindow(5100, 10100)});
}

TEST(TumblingWindowAssignerTest, RejectsInvalidSizeAndKeepsTimeMode)
{
    EXPECT_THROW(TumblingWindowAssigner(-1, 0, true), std::logic_error);

    TumblingWindowAssigner assigner(5000, 0, true);
    EXPECT_TRUE(assigner.isEventTime());

    std::unique_ptr<TumblingWindowAssigner> eventTime(assigner.withEventTime());
    std::unique_ptr<TumblingWindowAssigner> processingTime(assigner.withProcessingTime());
    EXPECT_TRUE(eventTime->isEventTime());
    EXPECT_FALSE(processingTime->isEventTime());
}

TEST(SlidingWindowAssignerTest, WindowAssignmentAndPanesMatchFlink)
{
    SlidingWindowAssigner assigner(5000, 1000, 0, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 0L),
        {TimeWindow(0, 5000), TimeWindow(-1000, 4000), TimeWindow(-2000, 3000),
         TimeWindow(-3000, 2000), TimeWindow(-4000, 1000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 4999L),
        {TimeWindow(4000, 9000), TimeWindow(3000, 8000), TimeWindow(2000, 7000),
         TimeWindow(1000, 6000), TimeWindow(0, 5000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5000L),
        {TimeWindow(5000, 10000), TimeWindow(4000, 9000), TimeWindow(3000, 8000),
         TimeWindow(2000, 7000), TimeWindow(1000, 6000)});

    EXPECT_EQ(assigner.assignPane(nullptr, 0L), TimeWindow(0, 1000));
    EXPECT_EQ(assigner.assignPane(nullptr, 4999L), TimeWindow(4000, 5000));
    EXPECT_EQ(assigner.assignPane(nullptr, 5000L), TimeWindow(5000, 6000));

    AssertWindowsEq(assigner.splitIntoPanes(TimeWindow(0, 5000)),
        {TimeWindow(0, 1000), TimeWindow(1000, 2000), TimeWindow(2000, 3000),
         TimeWindow(3000, 4000), TimeWindow(4000, 5000)});
    AssertWindowsEq(assigner.splitIntoPanes(TimeWindow(3000, 8000)),
        {TimeWindow(3000, 4000), TimeWindow(4000, 5000), TimeWindow(5000, 6000),
         TimeWindow(6000, 7000), TimeWindow(7000, 8000)});

    EXPECT_EQ(assigner.getLastWindow(TimeWindow(4000, 5000)), TimeWindow(4000, 9000));
    EXPECT_EQ(assigner.getLastWindow(TimeWindow(2000, 3000)), TimeWindow(2000, 7000));
}

TEST(SlidingWindowAssignerTest, WindowAssignmentWithOffsetMatchesFlink)
{
    SlidingWindowAssigner assigner(5000, 1000, 100, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 100L),
        {TimeWindow(100, 5100), TimeWindow(-900, 4100), TimeWindow(-1900, 3100),
         TimeWindow(-2900, 2100), TimeWindow(-3900, 1100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5099L),
        {TimeWindow(4100, 9100), TimeWindow(3100, 8100), TimeWindow(2100, 7100),
         TimeWindow(1100, 6100), TimeWindow(100, 5100)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 5100L),
        {TimeWindow(5100, 10100), TimeWindow(4100, 9100), TimeWindow(3100, 8100),
         TimeWindow(2100, 7100), TimeWindow(1100, 6100)});

    EXPECT_EQ(assigner.assignPane(nullptr, 100L), TimeWindow(100, 1100));
    EXPECT_EQ(assigner.assignPane(nullptr, 5099L), TimeWindow(4100, 5100));
    EXPECT_EQ(assigner.assignPane(nullptr, 5100L), TimeWindow(5100, 6100));

    AssertWindowsEq(assigner.splitIntoPanes(TimeWindow(1100, 6100)),
        {TimeWindow(1100, 2100), TimeWindow(2100, 3100), TimeWindow(3100, 4100),
         TimeWindow(4100, 5100), TimeWindow(5100, 6100)});
    AssertWindowsEq(assigner.splitIntoPanes(TimeWindow(3100, 8100)),
        {TimeWindow(3100, 4100), TimeWindow(4100, 5100), TimeWindow(5100, 6100),
         TimeWindow(6100, 7100), TimeWindow(7100, 8100)});

    EXPECT_EQ(assigner.getLastWindow(TimeWindow(4100, 5100)), TimeWindow(4100, 9100));
    EXPECT_EQ(assigner.getLastWindow(TimeWindow(2100, 3100)), TimeWindow(2100, 7100));
}

TEST(SlidingWindowAssignerTest, RejectsInvalidParametersAndKeepsTimeMode)
{
    EXPECT_THROW(SlidingWindowAssigner(-2000, 1000, 0, true), std::logic_error);
    EXPECT_THROW(SlidingWindowAssigner(2000, -1000, 0, true), std::logic_error);

    SlidingWindowAssigner assigner(5000, 1000, 0, true);
    EXPECT_TRUE(assigner.isEventTime());

    std::unique_ptr<SlidingWindowAssigner> eventTime(assigner.withEventTime());
    std::unique_ptr<SlidingWindowAssigner> processingTime(assigner.withProcessingTime());
    EXPECT_TRUE(eventTime->isEventTime());
    EXPECT_FALSE(processingTime->isEventTime());
}
