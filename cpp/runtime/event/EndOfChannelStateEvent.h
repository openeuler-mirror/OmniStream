/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once
#include <typeinfo>
#include "RuntimeEvent.h"

namespace omnistream
{
    class EndOfChannelStateEvent : public RuntimeEvent
    {
    public:
        static EndOfChannelStateEvent &getInstance();
        // {
        //     static EndOfChannelStateEvent instance;
        // }

        static int hashCode()
        {
            return 1965146670;
        }

        static bool equals(RuntimeEvent &other)
        {
            return typeid(other) == typeid(EndOfChannelStateEvent);
        }

    private:
        EndOfChannelStateEvent() {}
    };
} // namespace omnistream
