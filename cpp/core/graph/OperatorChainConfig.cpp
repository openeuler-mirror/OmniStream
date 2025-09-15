/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OperatorChainConfig.h"

void OperatorChainConfig::appendOperatorConfig(OperatorConfig config)
{
    OpChainConfig_.push_back(config);
}

OperatorConfig OperatorChainConfig::getOperatorConfig(int i)
{
    return OpChainConfig_[i];
}

int OperatorChainConfig::size()
{
    return (int)OpChainConfig_.size();
}
