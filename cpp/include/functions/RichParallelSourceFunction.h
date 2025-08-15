/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_RICHPARALLELSOURCEFUNCTION_H
#define FLINK_TNEL_RICHPARALLELSOURCEFUNCTION_H

#include "AbstractRichFunction.h"
#include "ParallelSourceFunction.h"

template<typename K>
class RichParallelSourceFunction : public ParallelSourceFunction<K> {
public:
    RichParallelSourceFunction ()  = default;
};

#endif //FLINK_TNEL_RICHPARALLELSOURCEFUNCTION_H
