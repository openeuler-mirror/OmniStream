/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef STATEKEYCONTEXT_H
#define STATEKEYCONTEXT_H

#include "table/data/RowData.h"

class StateKeyContext {
public:
    virtual void setCurrentKey(RowData *key) = 0;
};

#endif
