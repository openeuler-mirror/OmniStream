/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/3/25.
//

#include "OmniStreamTaskNetworkOutput.h"
#include "runtime/metrics/Counter.h"

namespace omnistream {
    OmniStreamTaskNetworkOutput::OmniStreamTaskNetworkOutput(
        Input* operator_,
        std::shared_ptr<omnistream::SimpleCounter> & numRecordsIn)
        : operator_(operator_), numRecordsIn(numRecordsIn) {
    }
}