/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef ORIGINALNETWORKBUFFERRECYCLER_H
#define ORIGINALNETWORKBUFFERRECYCLER_H
#include "BufferRecycler.h"
#include "core/memory/MemorySegment.h"
#include <condition_variable>
#include <queue>

namespace omnistream {
    class OriginalNetworkBufferRecycler : public BufferRecycler {
    public:
        OriginalNetworkBufferRecycler() = default;
        ~OriginalNetworkBufferRecycler() override = default;

        void recycle(Segment *segment) override
        {
            auto memorySegment = reinterpret_cast<MemorySegment*>(segment);
            if (memorySegment) {
                long address = reinterpret_cast<long>(memorySegment->getAll());

                std::lock_guard<std::mutex> lock(queue_mutex);
                originalNetworkBufferQueue.push(address);
                queue_cv.notify_one();
            }
        };

        [[nodiscard]] std::string toString() const override
        {
            return "OriginalNetworkBufferRecycler";
        };

        long getRecycleBufferAddress()
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [this] { return !originalNetworkBufferQueue.empty(); });
            long address = originalNetworkBufferQueue.front();
            originalNetworkBufferQueue.pop();
            return address;
        }

        void stop()
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            originalNetworkBufferQueue.push(-9999);
            queue_cv.notify_one();
        }

    private:
        std::queue<long> originalNetworkBufferQueue;
        std::mutex queue_mutex;
        std::condition_variable queue_cv;
    };
}
#endif // ORIGINALNETWORKBUFFERRECYCLER_H
