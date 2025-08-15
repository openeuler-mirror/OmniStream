/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "basictypes/Double.h"
Double::Double() = default;
Double::~Double() = default;
Double::Double(double val)
{
    this->val = val;
}
Double* Double::valueOf(double d)
{
    return new Double(d);
};

double Double::jsonValue()
{
    return val;
}