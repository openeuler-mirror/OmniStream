/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/19/25.
//

#include "ConstraintEnforcer.h"


void ConstraintEnforcer::processElement(StreamRecord *record)
{
    LOG("ConstraintEnforcer::processElement(StreamRecord *record)")
    output->collect(record);
}

void ConstraintEnforcer::processBatch(StreamRecord *record)
{
    LOG("ConstraintEnforcer::processBatch(StreamRecord *record)")
    output->collect(record);
}