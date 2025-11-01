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
#ifndef OMNISTREAM_ALTERNATINGWAITINGFORFIRSTBARRIERUNALIGNED_H
#define OMNISTREAM_ALTERNATINGWAITINGFORFIRSTBARRIERUNALIGNED_H

#include "ChannelState.h"
#include "BarrierHandlerState.h"

class AlternatingWaitingForFirstBarrierUnaligned : public BarrierHandlerState {
public:
    AlternatingWaitingForFirstBarrierUnaligned(bool alternating, ChannelState state);

    BarrierHandlerState* BarrierReceived(
        Controller* controller,
        InputChannelInfo channelInfo,
        CheckpointBarrier* barrier,
        bool markChannelBlocked) override;

    BarrierHandlerState* AlignedCheckpointTimeout(
        Controller*,
        CheckpointBarrier*) override;

    BarrierHandlerState* FinishCheckpoint() override;

private:
    bool alternating_;
    ChannelState state_;
};

#endif // OMNISTREAM_ALTERNATINGWAITINGFORFIRSTBARRIERUNALIGNED_H