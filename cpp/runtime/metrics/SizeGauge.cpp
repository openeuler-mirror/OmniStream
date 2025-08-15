/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "SizeGauge.h"

namespace omnistream {
    SizeGauge::SizeGauge() : supplier(nullptr)
    {
    }

    void SizeGauge::RegisterSupplier(SizeSupplier sp)
    {
        this->supplier = sp;
    }

    int SizeGauge::GetValue() const
    {
        return supplier ? supplier() : 0; // assumed empty queue size
    }
} // namespace omnistream
