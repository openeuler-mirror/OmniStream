/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef STOPMODE_H
#define STOPMODE_H
#include <string>

namespace omnistream {
    enum class StopMode : int {
        DRAIN = 1,
        NO_DRAIN
    };

    std::string StopModeToString(StopMode mode);

}

#endif // STOPMODE_H
