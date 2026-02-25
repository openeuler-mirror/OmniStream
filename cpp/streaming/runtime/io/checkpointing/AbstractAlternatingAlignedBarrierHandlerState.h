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
#ifndef OMNISTREAM_ABSTRACTALIGNEDBARRIERHANDLERSTATE_H
#define OMNISTREAM_ABSTRACTALIGNEDBARRIERHANDLERSTATE_H

#include "BarrierHandlerState.h"
#include "ChannelState.h"
#include "runtime/io/network/api/CheckpointBarrier.h"

class AbstractAlternatingAlignedBarrierHandlerState : public BarrierHandlerState {
public:
    explicit AbstractAlternatingAlignedBarrierHandlerState(ChannelState state);
    ~AbstractAlternatingAlignedBarrierHandlerState() override = default;

    BarrierHandlerState* BarrierReceived(
        Controller* controller,
        InputChannelInfo channelInfo,
        CheckpointBarrier* barrier,
        bool markChannelBlocked) override;

    BarrierHandlerState* AnnouncementReceived(
        Controller* controller,
        InputChannelInfo channelInfo,
        int sequenceNumber) override;

    BarrierHandlerState* AlignedCheckpointTimeout(
        Controller* controller,
        CheckpointBarrier* barrier) override = 0;

    BarrierHandlerState* FinishCheckpoint() override;

protected:
    virtual BarrierHandlerState* TransitionAfterBarrierReceived(ChannelState state) = 0;
    ChannelState state;
};

#endif // OMNISTREAM_ABSTRACTALIGNEDBARRIERHANDLERSTATE_H