/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OPERATOREVENTHANDLER_H
#define OMNISTREAM_OPERATOREVENTHANDLER_H

#include "OperatorEvent.h"

class OperatorEventHandler {
public:
    virtual void handleOperatorEvent(const std::string& eventString) = 0;
};

#endif // OMNISTREAM_OPERATOREVENTHANDLER_H
