/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_WATERMARKSTATUS_H
#define FLINK_TNEL_WATERMARKSTATUS_H

#include "functions/StreamElement.h"
#include "common.h"

class WatermarkStatus : public StreamElement
{
public:
    const int status_;
    static const int idleStatus = -1;
    static const int activeStatus = 0;
    static WatermarkStatus* idle;
    static WatermarkStatus* active;
    explicit WatermarkStatus(int status) noexcept : status_(status)
    {
        if (status != idleStatus && status != activeStatus)
        {
            THROW_LOGIC_EXCEPTION("Invalid status value for WatermarkStatus");
        }
    }

    bool IsIdle()
    {
        return status_ == idleStatus;
    }

    bool IsActive()
    {
        return !IsIdle();
    }

    bool Equals(WatermarkStatus *other)
    {
        return other->GetStatus() == status_;
    }

    bool Equals(int other)
    {
        return other == status_;
    }

    int GetStatus()
    {
        return status_;
    }
};
#endif // FLINK_TNEL_WATERMARKSTATUS_H
