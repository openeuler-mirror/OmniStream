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
#ifndef OMNISTREAM_ALTERNATINGCOLLECTINGBARRIERSUNALIGNED_H
#define OMNISTREAM_ALTERNATINGCOLLECTINGBARRIERSUNALIGNED_H

#include "BarrierHandlerState.h"
#include "ChannelState.h"

class AlternatingCollectingBarriersUnaligned : public BarrierHandlerState {
public:
    AlternatingCollectingBarriersUnaligned(bool alternating, ChannelState state, long checkpointId);
    ~AlternatingCollectingBarriersUnaligned() override = default;

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
    long checkpointId_;
};

#endif // OMNISTREAM_ALTERNATINGCOLLECTINGBARRIERSUNALIGNED_H