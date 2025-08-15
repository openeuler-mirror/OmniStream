/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/18/25.
//

#ifndef REGULAROPERATORCHAIN_H
#define REGULAROPERATORCHAIN_H
#include <tasks/OperatorChain.h>
#include "taskmanager/OmniRuntimeEnvironment.h"


namespace omnistream {
    class RegularOperatorChain : public OperatorChainV2 {
    public:
            RegularOperatorChain(const std::weak_ptr<OmniStreamTask> &containingTask,
                std::shared_ptr<RecordWriterDelegateV2> recordWriterDelegate)
                : OperatorChainV2(containingTask, recordWriterDelegate) {
            }
    };
}
#endif //REGULAROPERATORCHAIN_H
