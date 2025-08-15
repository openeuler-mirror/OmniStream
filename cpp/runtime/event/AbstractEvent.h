/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
*/

#ifndef ABSTRACT_EVENT_H
#define ABSTRACT_EVENT_H

#include <common.h>

namespace omnistream {
    class AbstractEvent {
    public:
        static const int abstractEvent;
        static const int endOfPartition;
        static const int endOfData;

        virtual int GetEventClassID()
        {
            NOT_IMPL_EXCEPTION
        }

        virtual std::string GetEventClassName()
        {
            NOT_IMPL_EXCEPTION
        }
    };
} // namespace omnistream

#endif
