/*
* Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 */

#ifndef TASKOPERATOREVENTGATEWAYBRIDGE_H
#define TASKOPERATOREVENTGATEWAYBRIDGE_H
#include <string>

#include "jobgraph/OperatorID.h"
#include "operators/coordination/OperatorEvent.h"


namespace omnistream {
    class TaskOperatorEventGatewayBridge {
    public:
        virtual ~TaskOperatorEventGatewayBridge() = default;
        virtual void sendOperatorEventToCoordinator(std::string operatorId, std::string event)=0;
    };
}

#endif // TASKOPERATOREVENTGATEWAYBRIDGE_H
