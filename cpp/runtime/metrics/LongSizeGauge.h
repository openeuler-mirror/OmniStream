/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#ifndef LONG_SIZE_GAUGE_H
#define LONG_SIZE_GAUGE_H
#include <cstdint>
#include <functional>
#include "Gauge.h"

namespace omnistream {
    // 64-bit counterpart of SizeGauge for byte-valued metrics (keyed-state data sizes)
    // that can exceed INT_MAX. SizeGauge stays int (Flink convention, shared by counts/buffer pools).
    class LongSizeGauge : public Gauge<int64_t> {
    public:
        using LongSizeSupplier = std::function<int64_t()>;
        LongSizeGauge();
        void RegisterSupplier(LongSizeSupplier supplier);
        int64_t GetValue() const override;

    private:
        LongSizeSupplier supplier;
    };
} // namespace omnistream
#endif // LONG_SIZE_GAUGE_H
