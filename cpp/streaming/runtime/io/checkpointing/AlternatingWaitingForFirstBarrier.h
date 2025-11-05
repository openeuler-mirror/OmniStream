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
#ifndef OMNISTREAM_ALTERNATINGWAITINGFORFIRSTBARRIER_H
#define OMNISTREAM_ALTERNATINGWAITINGFORFIRSTBARRIER_H

#include "AbstractAlternatingAlignedBarrierHandlerState.h"

class AlternatingWaitingForFirstBarrier : public AbstractAlternatingAlignedBarrierHandlerState {
public:
    explicit AlternatingWaitingForFirstBarrier(ChannelState state);
    
    ~AlternatingWaitingForFirstBarrier() override = default;

    BarrierHandlerState* AlignedCheckpointTimeout(
        Controller* controller,
        CheckpointBarrier* barrier) override;

protected:
    BarrierHandlerState* TransitionAfterBarrierReceived(ChannelState state) override;
};

#endif // OMNISTREAM_ALTERNATINGWAITINGFORFIRSTBARRIER_H