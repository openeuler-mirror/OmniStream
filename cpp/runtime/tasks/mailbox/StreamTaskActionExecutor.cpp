/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/22/25.
//

// StreamTaskActionExecutor.cpp
#include "StreamTaskActionExecutor.h"

namespace omnistream {

    // Initialize the static member
    std::shared_ptr<StreamTaskActionExecutor> StreamTaskActionExecutor::IMMEDIATE =
        std::make_shared<ImmediateStreamTaskActionExecutor>();

} // namespace omnistream

