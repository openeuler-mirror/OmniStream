/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_CHAINING_STRATEGY_H
#define FLINK_TNEL_CHAINING_STRATEGY_H

enum class ChainingStrategy {
    ALWAYS,
    NEVER,
    HEAD,
    HEAD_WITH_SOURCES
};

#endif // FLINK_TNEL_CHAINING_STRATEGY_H
