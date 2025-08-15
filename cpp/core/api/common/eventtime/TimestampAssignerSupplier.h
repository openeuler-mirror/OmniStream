/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_TIMESTAMPASSIGNERSUPPLIER_H
#define OMNISTREAM_TIMESTAMPASSIGNERSUPPLIER_H

#include "TimestampAssigner.h"

class TimestampAssignerSupplier {
public:
    class Context {
    };

    virtual TimestampAssigner* CreateTimestampAssigner() = 0;
};
#endif // OMNISTREAM_TIMESTAMPASSIGNERSUPPLIER_H
