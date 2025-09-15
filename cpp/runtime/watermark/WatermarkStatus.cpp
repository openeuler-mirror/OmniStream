/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "WatermarkStatus.h"

WatermarkStatus* WatermarkStatus::idle = new WatermarkStatus(WatermarkStatus::idleStatus);
WatermarkStatus* WatermarkStatus::active = new WatermarkStatus(WatermarkStatus::activeStatus);