#include <gtest/gtest.h>
#include <vector>
#include <climits>

#include "runtime/watermark/StatusWatermarkValve.h"
#include "streaming/api/watermark/Watermark.h"
#include "runtime/watermark/WatermarkStatus.h"

class MockStatusWatermarkValveOutput {
public:
    std::vector<int64_t> emittedWatermarks;
    std::vector<WatermarkStatus*> emittedWatermarkStatuses;

    void emitWatermark(Watermark* watermark) {
        if (watermark != nullptr) {
            emittedWatermarks.push_back(watermark->getTimestamp());
        }
    }

    void emitWatermarkStatus(WatermarkStatus* status) {
        if (status != nullptr) {
            emittedWatermarkStatuses.push_back(status);
        }
    }

    int64_t getLastWatermark() {
        if (emittedWatermarks.empty()) {
            return INT64_MIN;
        }
        return emittedWatermarks.back();
    }

    WatermarkStatus* getLastWatermarkStatus() {
        if (emittedWatermarkStatuses.empty()) {
            return nullptr;
        }
        return emittedWatermarkStatuses.back();
    }
};

TEST(StatusWatermarkValveTest, ConstructorWithInvalidChannels) {
    EXPECT_THROW(StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(0), std::logic_error);
    EXPECT_THROW(StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(-1), std::logic_error);
}

TEST(StatusWatermarkValveTest, InputWatermarkWithInvalidChannelIndex) {
    StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(2);
    MockStatusWatermarkValveOutput output;

    Watermark wm(100);
    EXPECT_THROW(valve.inputWatermark(&wm, -1, &output), std::logic_error);
    EXPECT_THROW(valve.inputWatermark(&wm, 2, &output), std::logic_error);
}

TEST(StatusWatermarkValveTest, InputWatermarkToSingleChannel) {
    StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(1);
    MockStatusWatermarkValveOutput output;

    Watermark wm1(100);
    valve.inputWatermark(&wm1, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 100);

    Watermark wm2(200);
    valve.inputWatermark(&wm2, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 200);
}

TEST(StatusWatermarkValveTest, InputWatermarkToMultipleChannels) {
    StatusWatermarkValve<MockStatusWatermarkValveOutput> valve(2);
    MockStatusWatermarkValveOutput output;

    Watermark wm1(100);
    valve.inputWatermark(&wm1, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), INT64_MIN);

    Watermark wm2(150);
    valve.inputWatermark(&wm2, 1, &output);
    EXPECT_EQ(output.getLastWatermark(), 100);

    Watermark wm3(200);
    valve.inputWatermark(&wm3, 1, &output);
    EXPECT_EQ(output.getLastWatermark(), 100);

    Watermark wm4(150);
    valve.inputWatermark(&wm4, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 150);

    Watermark wm5(180);
    valve.inputWatermark(&wm5, 0, &output);
    EXPECT_EQ(output.getLastWatermark(), 180);
}