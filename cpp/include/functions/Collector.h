/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef FLINK_TNEL_COLLECTOR_H
#define FLINK_TNEL_COLLECTOR_H

class Collector {
public:
    virtual ~Collector() = default;
    virtual void collect(void* record) = 0;
    virtual void close() = 0;
};

#endif //FLINK_TNEL_COLLECTOR_H
