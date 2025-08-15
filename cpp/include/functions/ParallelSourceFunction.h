/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_PARALLELSOURCEFUNCTION_H
#define FLINK_TNEL_PARALLELSOURCEFUNCTION_H
#include "SourceFunction.h"

template<typename K>
class ParallelSourceFunction : public SourceFunction<K> {
};

#endif //FLINK_TNEL_PARALLELSOURCEFUNCTION_H