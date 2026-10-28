/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include <gtest/gtest.h>
#include <future>
#include <thread>
#include <vector>
#include "runtime/io/network/api/writer/V2/OutputFlusher.h"

TEST(OutputFlusherTest, ConcurrentTerminateAndRepeatedClose)
{
    for (int iteration = 0; iteration < 32; ++iteration) {
        omnistream::OutputFlusher flusher("concurrent-close-test", 10, nullptr);
        flusher.start();

        std::promise<void> release;
        auto ready = release.get_future().share();
        std::vector<std::thread> callers;
        for (int caller = 0; caller < 8; ++caller) {
            callers.emplace_back([&flusher, ready]() {
                ready.wait();
                flusher.terminate();
            });
        }
        release.set_value();
        for (auto& caller : callers) {
            caller.join();
        }

        // Explicit close and destruction must remain safe after concurrent callers finish.
        flusher.terminate();
    }
    SUCCEED();
}
