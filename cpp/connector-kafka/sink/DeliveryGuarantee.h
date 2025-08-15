/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_DELIVERYGUARANTEE_H
#define FLINK_BENCHMARK_DELIVERYGUARANTEE_H

enum class DeliveryGuarantee {
    EXACTLY_ONCE,
    NONE,
    AT_LEAST_ONCE
};

#endif // FLINK_BENCHMARK_DELIVERYGUARANTEE_H
