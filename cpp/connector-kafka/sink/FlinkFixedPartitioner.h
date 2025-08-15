/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_FLINKFIXEDPARTITIONER_H
#define FLINK_BENCHMARK_FLINKFIXEDPARTITIONER_H

class FlinkFixedPartitioner {
public:
    FlinkFixedPartitioner(int parallelInstanceId) {};

private:
    int parallelInstanceId_;
};

#endif // FLINK_BENCHMARK_FLINKFIXEDPARTITIONER_H
