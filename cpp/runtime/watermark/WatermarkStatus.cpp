/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by q00649235 on 2025/3/28.
//

#include "WatermarkStatus.h"

WatermarkStatus* WatermarkStatus::idle = new WatermarkStatus(WatermarkStatus::idleStatus);
WatermarkStatus* WatermarkStatus::active = new WatermarkStatus(WatermarkStatus::activeStatus);