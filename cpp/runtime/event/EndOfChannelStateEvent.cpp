/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "EndOfChannelStateEvent.h"
 
namespace omnistream
{
    EndOfChannelStateEvent& EndOfChannelStateEvent::getInstance()
    {
        static EndOfChannelStateEvent instance;
        return instance;
    }
} // namespace omnistream
