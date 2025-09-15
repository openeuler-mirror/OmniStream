/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "StreamTaskActionExecutor.h"

namespace omnistream {

    // Initialize the static member
    std::shared_ptr<StreamTaskActionExecutor> StreamTaskActionExecutor::IMMEDIATE =
        std::make_shared<ImmediateStreamTaskActionExecutor>();

} // namespace omnistream

