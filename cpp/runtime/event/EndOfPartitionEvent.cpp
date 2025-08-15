/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "EndOfPartitionEvent.h"

namespace omnistream
{
    EndOfPartitionEvent& EndOfPartitionEvent::getInstance()
    {
        static EndOfPartitionEvent instance;
        return instance;
    }
}