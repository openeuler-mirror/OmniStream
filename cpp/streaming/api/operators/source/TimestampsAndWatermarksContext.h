/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_TIMESTAMPSANDWATERMARKSCONTEXT_H
#define OMNISTREAM_TIMESTAMPSANDWATERMARKSCONTEXT_H

#include "core/api/common/eventtime/TimestampAssignerSupplier.h"
#include "core/api/common/eventtime/WatermarkGeneratorSupplier.h"

class TimestampsAndWatermarksContext : public TimestampAssignerSupplier::Context,
    public WatermarkGeneratorSupplier::Context {
};


#endif // OMNISTREAM_TIMESTAMPSANDWATERMARKSCONTEXT_H
