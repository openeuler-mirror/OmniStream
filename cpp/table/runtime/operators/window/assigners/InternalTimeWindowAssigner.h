/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef INTERNALTIMEWINDOWASSIGNER_H
#define INTERNALTIMEWINDOWASSIGNER_H

#pragma once

class InternalTimeWindowAssigner {
public:
    virtual ~InternalTimeWindowAssigner() = default;

    virtual InternalTimeWindowAssigner *WithEventTime() = 0;

    virtual InternalTimeWindowAssigner *WithProcessingTime() = 0;
};

#endif
