/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef FLINK_TNEL_LocalDateTime_H
#define FLINK_TNEL_LocalDateTime_H

#include <chrono>
#include <ctime>
#include "table/runtime/operators/window/slicing/SliceAssigners.h"
#include "java_time_temporal_TemporalAccessor.h"
#include "java_time_Instant.h"

#define SEC_TO_NSEC 1000000000

class LocalDateTime : public TemporalAccessor {
public:
    static LocalDateTime *ofInstant(Instant *instant, omnistream::ZoneId *zoneId);

    std::tm *getTm();

private:
    std::tm now_tm;
};

#endif // FLINK_TNEL_LocalDateTime_H
