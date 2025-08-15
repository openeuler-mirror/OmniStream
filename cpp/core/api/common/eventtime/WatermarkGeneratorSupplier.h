/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_WATERMARKGENERATORSUPPLIER_H
#define OMNISTREAM_WATERMARKGENERATORSUPPLIER_H

#include "WatermarkGenerator.h"

class WatermarkGeneratorSupplier {
public:
    class Context {
    };

    virtual WatermarkGenerator* CreateWatermarkGenerator() = 0;
};

#endif // OMNISTREAM_WATERMARKGENERATORSUPPLIER_H
