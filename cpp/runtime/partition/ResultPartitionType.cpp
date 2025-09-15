/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ResultPartitionType.h"

namespace omnistream
{
    const int ResultPartitionType::PIPELINED = ResultPartitionType::encode(true, true, false, false, false);
    const int ResultPartitionType::PIPELINED_BOUNDED = ResultPartitionType::encode(true, true, true, false, false);
    const int ResultPartitionType::PIPELINED_APPROXIMATE = ResultPartitionType::encode(true, true, true, false, true);
    const int ResultPartitionType::BLOCKING_PERSISTENT = ResultPartitionType::encode(false, false, false, true, true);
    const int ResultPartitionType::BLOCKING = ResultPartitionType::encode(false, false, false, false, true);

    int ResultPartitionType::encode(bool isPipelined, bool hasBackPressure, bool isBounded, bool isPersistent,
                                    bool isReconnectable)
    {
        int flags = 0;

        if (isPipelined)
        {
            flags |= PIPELINED_MASK;
        }
        if (hasBackPressure)
        {
            flags |= BACK_PRESSURE_MASK;
        }
        if (isBounded)
        {
            flags |= BOUNDED_MASK;
        }
        if (isPersistent)
        {
            flags |= PERSISTENT_MASK;
        }
        if (isReconnectable)
        {
            flags |= RECONNECTABLE_MASK;
        }

        return flags;
    }

    std::string ResultPartitionType::getNameByType(int type)
    {
        if (type == PIPELINED)
        {
            return "PIPELINED";
        } else if (type == PIPELINED_BOUNDED)
        {
            return "PIPELINED_BOUNDED";
        } else if (type == BLOCKING_PERSISTENT)
        {
            return "BLOCKING_PERSISTENT";
        } else if (type == BLOCKING)
        {
            return "BLOCKING";
        } else if (type == PIPELINED_APPROXIMATE)
        {
            return "PIPELINED_APPROXIMATE";
        }  else
        {
            return "UNKNOWN";
        }
    }
} // namespace omnistream
