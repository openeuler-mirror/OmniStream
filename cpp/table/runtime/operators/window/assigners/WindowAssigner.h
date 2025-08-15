/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef WINDOWASSIGNER_H
#define WINDOWASSIGNER_H

#pragma once
#include "table/data/RowData.h"

template<typename W>
class WindowAssigner {
public:
    WindowAssigner() = default;

    virtual ~WindowAssigner() = default;

    virtual void Open() {}

    virtual std::vector<W> AssignWindows(const RowData *rowData, long timestamp) = 0;

    virtual bool IsEventTime() const = 0;
};

#endif
