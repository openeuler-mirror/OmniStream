/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#ifndef EVENTDATABUFFERRECYCLER_H
#define EVENTDATABUFFERRECYCLER_H

#include "BufferRecycler.h"
#include "core/include/common.h"
namespace omnistream {
    class EventDataBufferRecycler : public BufferRecycler {
    public:
        ~EventDataBufferRecycler() override = default;

        static std::shared_ptr<EventDataBufferRecycler> GetInstance()
        {
            static std::shared_ptr<EventDataBufferRecycler> instance =
                std::make_shared<EventDataBufferRecycler>();
            return instance;
        }

        // Recycle the segment
        void recycle(std::shared_ptr<Segment> objectBuffer) override;

        // Convert to string representation
        [[nodiscard]] std::string toString() const override
        {
            return "EventDataBufferRecycler";
        }
    };
} // namespace omnistream

#endif // EVENTDATABUFFERRECYCLER_H
