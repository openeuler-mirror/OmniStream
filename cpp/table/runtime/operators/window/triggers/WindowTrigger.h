/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef WINDOWTRIGGER_H
#define WINDOWTRIGGER_H

#pragma once

#include <iostream>
#include <chrono>
#include <memory>

#include "Trigger.h"

template<typename W>
class WindowTrigger : public Trigger<W> {
public:
    WindowTrigger() = default;

    ~WindowTrigger() override = default;

    long TriggerTime(const W &window)
    {
        return window.maxTimestamp();
    }

protected:
    typename Trigger<W>::TriggerContext *ctx;
};

#endif
