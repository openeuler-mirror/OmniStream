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
#include "AbstractAlternatingAlignedBarrierHandlerState.h"
#include "AlternatingWaitingForFirstBarrier.h"

AbstractAlternatingAlignedBarrierHandlerState::AbstractAlternatingAlignedBarrierHandlerState(ChannelState state)
    : state(std::move(state)) {}

BarrierHandlerState* AbstractAlternatingAlignedBarrierHandlerState::BarrierReceived(
    Controller* controller,
    InputChannelInfo channelInfo,
    CheckpointBarrier* barrier,
    bool markChannelBlocked)
{
    LOG_DEBUG("AbstractAlternatingAlignedBarrierHandlerState::BarrierReceived")
    if (barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        return AlignedCheckpointTimeout(controller, barrier)
            ->BarrierReceived(controller, channelInfo, barrier, markChannelBlocked);
    }

    state.removeSeenAnnouncement(channelInfo);

    if (markChannelBlocked) {
        state.BlockChannel(channelInfo);
    }

    if (controller->AllBarriersReceived()) {
        controller->InitInputsCheckpoint(*barrier);
        controller->TriggerGlobalCheckpoint(*barrier);
        return FinishCheckpoint();
    }

    return TransitionAfterBarrierReceived(state);
}

BarrierHandlerState* AbstractAlternatingAlignedBarrierHandlerState::AnnouncementReceived(Controller* /*controller*/,
                                                                                         InputChannelInfo channelInfo,
                                                                                         int sequenceNumber)
{
    // Only record the announcement; do NOT prioritize it here.
    // Prioritization happens when the aligned checkpoint times out.
    LOG("start AnnouncementReceived, ")
    state.addSeenAnnouncement(channelInfo, sequenceNumber);
    return this;
}

BarrierHandlerState* AbstractAlternatingAlignedBarrierHandlerState::FinishCheckpoint()
{
    state.UnblockAllChannels();
    return new AlternatingWaitingForFirstBarrier(state.EmptyState());
}