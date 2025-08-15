/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "WatermarkAlignmentParams.h"

const WatermarkAlignmentParams* WatermarkAlignmentParams::watermarkAlignmentDisabled
    = new WatermarkAlignmentParams(INT64_MAX, 0, "");

