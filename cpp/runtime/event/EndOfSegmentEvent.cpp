/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "EndOfSegmentEvent.h"

namespace omnistream
{
    EndOfSegmentEvent &EndOfSegmentEvent::getInstance()
    {
        static EndOfSegmentEvent instance;
        return instance;
    }
} // namespace omnistream