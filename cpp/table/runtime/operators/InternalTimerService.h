/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef INTERNALTIMERSERVICE_H
#define INTERNALTIMERSERVICE_H

#pragma once

template<typename N>
class InternalTimerService {
public:
    virtual ~InternalTimerService() = default;

    virtual long currentProcessingTime() = 0;

    /** Returns the current event-time watermark. */
    virtual long currentWatermark() = 0;

    /**
     * Registers a timer to be fired when processing time passes the given time. The nameSpace you
     * pass here will be provided when the timer fires.
     */
    virtual void registerProcessingTimeTimer(N nameSpace, long time) = 0;

    /** Deletes the timer for the given key and nameSpace. */
    virtual void deleteProcessingTimeTimer(N nameSpace, long time) = 0;

    virtual void registerEventTimeTimer(N nameSpace, long time) = 0;

    virtual void deleteEventTimeTimer(N nameSpace, long time) = 0;

    virtual void advanceWatermark(long time) = 0;
};

#endif