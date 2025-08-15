/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef FLINK_BENCHMARK_RECYCLABLE_H
#define FLINK_BENCHMARK_RECYCLABLE_H

#include <functional>
#include <memory>
#include "FlinkKafkaInternalProducer.h"

template <typename T>
class Recyclable {
public:
    Recyclable(T* object, std::function<void(T*)> recycler);
    ~Recyclable();

    T* GetObject();
    bool IsRecycled();
    void Close();
private:
    T* object;
    std::function<void(T*)> recycler;
};

#endif // FLINK_BENCHMARK_RECYCLABLE_H
