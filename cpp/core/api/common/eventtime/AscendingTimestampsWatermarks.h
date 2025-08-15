/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_ASCENDINGTIMESTAMPSWATERMARKS_H
#define OMNISTREAM_ASCENDINGTIMESTAMPSWATERMARKS_H

#include "BoundedOutOfOrdernessWatermarks.h"

class AscendingTimestampsWatermarks : public BoundedOutOfOrdernessWatermarks {
public:
    AscendingTimestampsWatermarks() : BoundedOutOfOrdernessWatermarks(0) {
    }

    ~AscendingTimestampsWatermarks() override = default;

};


#endif // OMNISTREAM_ASCENDINGTIMESTAMPSWATERMARKS_H
