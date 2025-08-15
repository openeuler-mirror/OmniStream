/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "functions/RichFunction.h"

RichFunction::~RichFunction() = default;

void RichFunction::open(const Configuration &parameters) {
}

void RichFunction::open(const OpenContext *openContext)
{
    open(Configuration());
}
