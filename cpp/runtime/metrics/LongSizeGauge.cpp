/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#include "LongSizeGauge.h"

namespace omnistream {
    LongSizeGauge::LongSizeGauge() : supplier(nullptr)
    {
    }

    void LongSizeGauge::RegisterSupplier(LongSizeSupplier sp)
    {
        this->supplier = sp;
    }

    int64_t LongSizeGauge::GetValue() const
    {
        return supplier ? supplier() : 0;
    }
} // namespace omnistream
