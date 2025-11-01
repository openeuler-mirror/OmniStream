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
#include "AlternatingCollectingBarriers.h"
#include "AlternatingCollectingBarriersUnaligned.h"

AlternatingCollectingBarriers::AlternatingCollectingBarriers(ChannelState state)
    : AbstractAlternatingAlignedBarrierHandlerState(std::move(state)) {}

BarrierHandlerState* AlternatingCollectingBarriers::AlignedCheckpointTimeout(
    Controller* controller,
    CheckpointBarrier* barrier)
{
    state.PrioritizeAllAnnouncements();
    CheckpointBarrier* unalignedBarrier = barrier->AsUnaligned();
    controller->InitInputsCheckpoint(*unalignedBarrier);

    for (CheckpointableInput* input : state.getInputs()) {
        input->CheckpointStarted(*unalignedBarrier);
    }

    controller->TriggerGlobalCheckpoint(*unalignedBarrier);
    return new AlternatingCollectingBarriersUnaligned(true, state, unalignedBarrier->GetId());
}

BarrierHandlerState* AlternatingCollectingBarriers::endOfPartitionReceived(
    Controller* controller,
    const InputChannelInfo& channelInfo)
{
    state.ChannelFinished(channelInfo);

    const CheckpointBarrier* pending = controller->GetPendingCheckpointBarrier();
    if (!pending) {
        throw std::runtime_error("At least one barrier should have been received.");
    }
    if (pending->GetCheckpointOptions()->IsUnalignedCheckpoint()) {
        throw std::runtime_error("Pending checkpoint should be aligned.");
    }

    if (controller->AllBarriersReceived()) {
        controller->InitInputsCheckpoint(*pending);
        controller->TriggerGlobalCheckpoint(*pending);
        return FinishCheckpoint();
    } else if (controller->IsTimedOut(*pending)) {
        return AlignedCheckpointTimeout(controller, const_cast<CheckpointBarrier*>(pending))
            ->BarrierReceived(controller, channelInfo, const_cast<CheckpointBarrier*>(pending), false);
    }
    return this;
}

BarrierHandlerState* AlternatingCollectingBarriers::TransitionAfterBarrierReceived(ChannelState state)
{
    return new AlternatingCollectingBarriers(std::move(state));
}