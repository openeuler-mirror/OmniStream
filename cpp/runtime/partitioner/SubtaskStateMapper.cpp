/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "SubtaskStateMapper.h"


namespace omnistream {
    SubtaskStateMapper *SubtaskStateMapper::create(Type type)
    {
        switch (type) {
            case ARBITRARY: return new ArbitraryMapper();
            case ROUND_ROBIN: return new RoundRobinMapper();
            default: return nullptr;
        }
    }
}
