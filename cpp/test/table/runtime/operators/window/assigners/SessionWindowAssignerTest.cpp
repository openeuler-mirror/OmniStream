#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

#include "table/runtime/operators/window/assigners/SessionWindowAssigner.h"

namespace {

void AssertWindowsEq(const std::vector<TimeWindow>& actual, const std::vector<TimeWindow>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual[i], expected[i]) << "at index " << i;
    }
}

void AssertWindowSetEq(const std::unordered_set<TimeWindow>& actual, const std::vector<TimeWindow>& expected)
{
    ASSERT_EQ(actual.size(), expected.size());
    for (const auto& window : expected) {
        EXPECT_NE(actual.find(window), actual.end()) << "missing window " << window;
    }
}

}

TEST(SessionWindowAssignerTest, WindowAssignmentMatchesFlink)
{
    SessionWindowAssigner assigner(5000, true);

    AssertWindowsEq(assigner.assignWindows(nullptr, 0L), {TimeWindow(0, 5000)});
    AssertWindowsEq(assigner.assignWindows(nullptr, 4999L), {TimeWindow(4999, 9999)});
    AssertWindowsEq(assigner.assignWindows(nullptr, -1L), {TimeWindow(-1, 4999)});
}

TEST(SessionWindowAssignerTest, MergeWindowsMergesIntersectingNeighbors)
{
    SessionWindowAssigner assigner(5000, true);
    std::set<TimeWindow> sortedWindows = {
        TimeWindow(0, 5000),
        TimeWindow(12000, 17000)
    };
    MergingWindowAssigner<TimeWindow>::MergeResultCollector callback;

    assigner.mergeWindows(TimeWindow(4000, 9000), &sortedWindows, callback);

    ASSERT_EQ(callback.size(), 1);
    auto it = callback.find(TimeWindow(0, 9000));
    ASSERT_NE(it, callback.end());
    AssertWindowSetEq(*it->second, {TimeWindow(0, 5000), TimeWindow(4000, 9000)});
}

TEST(SessionWindowAssignerTest, MergeWindowsMergesBothSidesWhenNewWindowConnectsThem)
{
    SessionWindowAssigner assigner(5000, true);
    std::set<TimeWindow> sortedWindows = {
        TimeWindow(0, 5000),
        TimeWindow(9000, 14000)
    };
    MergingWindowAssigner<TimeWindow>::MergeResultCollector callback;

    assigner.mergeWindows(TimeWindow(4000, 10000), &sortedWindows, callback);

    ASSERT_EQ(callback.size(), 1);
    auto it = callback.find(TimeWindow(0, 14000));
    ASSERT_NE(it, callback.end());
    AssertWindowSetEq(*it->second,
        {TimeWindow(0, 5000), TimeWindow(4000, 10000), TimeWindow(9000, 14000)});
}

TEST(SessionWindowAssignerTest, MergeWindowsSkipsDisjointWindows)
{
    SessionWindowAssigner assigner(5000, true);
    std::set<TimeWindow> sortedWindows = {
        TimeWindow(0, 5000),
        TimeWindow(12000, 17000)
    };
    MergingWindowAssigner<TimeWindow>::MergeResultCollector callback;

    assigner.mergeWindows(TimeWindow(6000, 11000), &sortedWindows, callback);

    EXPECT_TRUE(callback.empty());
}

TEST(SessionWindowAssignerTest, RejectsInvalidGapAndKeepsTimeMode)
{
    EXPECT_THROW(SessionWindowAssigner(0, true), std::logic_error);
    EXPECT_THROW(SessionWindowAssigner(-1, true), std::logic_error);

    SessionWindowAssigner assigner(5000, true);
    EXPECT_TRUE(assigner.isEventTime());

    std::unique_ptr<SessionWindowAssigner> eventTime(assigner.withEventTime());
    std::unique_ptr<SessionWindowAssigner> processingTime(assigner.withProcessingTime());
    EXPECT_TRUE(eventTime->isEventTime());
    EXPECT_FALSE(processingTime->isEventTime());
}
