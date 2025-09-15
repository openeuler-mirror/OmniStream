#include <gtest/gtest.h>
#include "streaming/runtime/tasks/SystemProcessingTimeService.h"

TEST(SystemProcessingTimeServiceTest, DISABLED_SelectFromRow) {
    SystemProcessingTimeService systemProcessingTimeService;

    class MockProcessingTimeCallback : public ProcessingTimeCallback {
    public:
        std::vector<long> timeVec;

        void OnProcessingTime(int64_t timestamp) override {
            std::cout << std::to_string(timestamp) << std::endl;
            timeVec.push_back(timestamp);
        }
    };

    MockProcessingTimeCallback* callback = new MockProcessingTimeCallback();
    systemProcessingTimeService.scheduleWithFixedDelay(callback, 2000, 3000);

    std::this_thread::sleep_for(std::chrono::milliseconds(8000));

    EXPECT_EQ(callback->timeVec.size(), 2);
    EXPECT_EQ(callback->timeVec[0], callback->timeVec[1] - 3000);
}

