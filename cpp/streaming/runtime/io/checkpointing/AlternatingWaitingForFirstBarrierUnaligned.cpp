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
#include "AlternatingWaitingForFirstBarrierUnaligned.h"
#include "AlternatingCollectingBarriersUnaligned.h"
#include "AlternatingWaitingForFirstBarrier.h"

AlternatingWaitingForFirstBarrierUnaligned::AlternatingWaitingForFirstBarrierUnaligned(
    bool alternating, ChannelState state)
    : alternating_(alternating), state_(std::move(state)) {}

BarrierHandlerState* AlternatingWaitingForFirstBarrierUnaligned::BarrierReceived(
    Controller* controller,
    InputChannelInfo channelInfo,
    CheckpointBarrier* barrier,
    bool markChannelBlocked)
{
    LOG_DEBUG("AlternatingWaitingForFirstBarrierUnaligned::BarrierReceived")
    if (markChannelBlocked && !barrier->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        state_.BlockChannel(channelInfo);
    }

    CheckpointBarrier* unalignedBarrier = barrier->AsUnaligned();
    controller->InitInputsCheckpoint(*unalignedBarrier);
    for (auto* input : state_.getInputs()) {
        input->CheckpointStarted(*unalignedBarrier);
    }
    controller->TriggerGlobalCheckpoint(*unalignedBarrier);

    if (controller->AllBarriersReceived()) {
        for (auto* input : state_.getInputs()) {
            input->CheckpointStopped(unalignedBarrier->GetId());
        }
        return FinishCheckpoint();
    }

    return new AlternatingCollectingBarriersUnaligned(
        alternating_, std::move(state_), unalignedBarrier->GetId());
}

BarrierHandlerState* AlternatingWaitingForFirstBarrierUnaligned::AlignedCheckpointTimeout(
    Controller*, CheckpointBarrier*)
{
    return this;
}

BarrierHandlerState* AlternatingWaitingForFirstBarrierUnaligned::FinishCheckpoint()
{
    state_.UnblockAllChannels();
    if (alternating_) {
        return new AlternatingWaitingForFirstBarrier(state_.EmptyState());
    } else {
        return new AlternatingWaitingForFirstBarrierUnaligned(false, state_.EmptyState());
    }
}