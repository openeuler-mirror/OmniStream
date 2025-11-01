/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef OMNISTREAM_WAITINGFORFIRSTBARRIER_H
#define OMNISTREAM_WAITINGFORFIRSTBARRIER_H


#include "runtime/io/checkpointing/BarrierHandlerState.h"

class WaitingForFirstBarrier : public BarrierHandlerState {
    BarrierHandlerState* BarrierReceived(Controller* controller, InputChannelInfo channelInfo,
        CheckpointBarrier* barrier, bool markChannelBlocked) override
        {
            // TTODO
            return nullptr;
        };

    BarrierHandlerState* AlignedCheckpointTimeout(Controller* controller, CheckpointBarrier* barrier) override
    {
        // TTODO
        return nullptr;
    };

    BarrierHandlerState* FinishCheckpoint() override
    {
        // TTODO
        return nullptr;
    };
};


#endif // OMNISTREAM_WAITINGFORFIRSTBARRIER_H
