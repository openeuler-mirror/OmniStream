/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/15/24.
//

#ifndef FLINK_TNEL_OPERATORCHAINCONFIG_H
#define FLINK_TNEL_OPERATORCHAINCONFIG_H


// now assume operator chain only a list, and the first case should only have one main operator
#include <vector>
#include "OperatorConfig.h"
using namespace omnistream;
class OperatorChainConfig {
public:
    void appendOperatorConfig(OperatorConfig);
    OperatorConfig getOperatorConfig(int i);
    int size();

private:
    std::vector<OperatorConfig> OpChainConfig_;

};


#endif  //FLINK_TNEL_OPERATORCHAINCONFIG_H
