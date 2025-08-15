/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "StopMode.h"
namespace omnistream {
    std::string StopModeToString(StopMode mode)
    {
        switch (mode) {
            case StopMode::DRAIN:
                return "DRAIN";
            case StopMode::NO_DRAIN:
                return "NO_DRAIN";
            default:
                return "UNKNOWN_STOP_MODE";
        }
    }
}