/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once
#include <typeinfo>
#include "RuntimeEvent.h"

namespace omnistream
{
    class EndOfPartitionEvent : public RuntimeEvent
    {
    public:
        static EndOfPartitionEvent &getInstance();
        // {
        //     static EndOfPartitionEvent instance;
        // }

        static int hashCode()
        {
            return 1965146673;
        }

        static bool equals(const RuntimeEvent &other)
        {
            return typeid(other) == typeid(EndOfPartitionEvent);
        }

    private:
        EndOfPartitionEvent() {}
    };
} // namespace omnistream
