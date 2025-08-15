/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once
#include <typeinfo>
#include "RuntimeEvent.h"

namespace omnistream
{
    class EndOfSegmentEvent : public RuntimeEvent
    {
    public:
        static EndOfSegmentEvent &getInstance();
        // {
        //     static EndOfSegmentEvent instance;
        // }

        static int hashCode()
        {
            return 1965146670;
        }

        static bool equals(const RuntimeEvent &other)
        {
            return typeid(other) == typeid(EndOfSegmentEvent);
        }

    private:
        EndOfSegmentEvent() {}
    };
} // namespace omnistream
