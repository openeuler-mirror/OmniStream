/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef SIZE_GAUGE_H
#define SIZE_GAUGE_H
#include <functional>
#include "Gauge.h"

namespace omnistream {
    class SizeGauge : public Gauge<int> {
    public:
        using SizeSupplier = std::function<int()>;
        SizeGauge();
        void RegisterSupplier(SizeSupplier supplier);
        int GetValue() const override;

    private:
        SizeSupplier supplier;
    };
} // namespace omnistream
#endif // SIZE_GAUGE_H
