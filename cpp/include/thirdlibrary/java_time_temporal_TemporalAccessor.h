/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef FLINK_TNEL_TemporalAccessor_H
#define FLINK_TNEL_TemporalAccessor_H

#include <ctime>
#include "basictypes/Object.h"

class TemporalAccessor : public Object {
public:
    TemporalAccessor();

    virtual ~TemporalAccessor();

    virtual std::tm *getTm() = 0;
};

#endif //FLINK_TNEL_TemporalAccessor_H
