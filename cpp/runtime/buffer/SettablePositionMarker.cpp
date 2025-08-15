/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "SettablePositionMarker.h"
/***
int SettablePositionMarker::get() const
{
    return position;
}

bool SettablePositionMarker::isFinished() const
{
    return position < 0;
}

int SettablePositionMarker::getCached() const
{
    return std::abs(position);
}

int SettablePositionMarker::markFinished()
{
    int currentPosition = getCached();
    int newValue = -currentPosition;
    if (newValue == 0)
    {
        newValue = INT_MIN;
    }
    set(newValue);
    return currentPosition;
}

void SettablePositionMarker::move(int offset)
{
    set(cachedPosition + offset);
}

void SettablePositionMarker::set(int value)
{
    cachedPosition = value;
}

void SettablePositionMarker::commit()
{
    position = cachedPosition;
}
**/