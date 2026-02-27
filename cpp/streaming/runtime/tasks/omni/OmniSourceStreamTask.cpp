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

#include "OmniSourceStreamTask.h"

#include "common.h"

namespace omnistream {

    StopMode FinishingReasonToStopMode(FinishingReason reason)
    {
        switch (reason) {
            case FinishingReason::END_OF_DATA:
                return StopMode::DRAIN;
            case FinishingReason::STOP_WITH_SAVEPOINT_DRAIN:
                return StopMode::DRAIN;
            case FinishingReason::STOP_WITH_SAVEPOINT_NO_DRAIN:
                return StopMode::NO_DRAIN;
            default:
                return StopMode::DRAIN;
        }
    }

    // Optional: For easier debugging or logging
    std::string FinishingReasonToString(FinishingReason reason)
    {
        switch (reason) {
            case FinishingReason::END_OF_DATA:
                return "END_OF_DATA";
            case FinishingReason::STOP_WITH_SAVEPOINT_DRAIN:
                return "STOP_WITH_SAVEPOINT_DRAIN";
            case FinishingReason::STOP_WITH_SAVEPOINT_NO_DRAIN:
                return "STOP_WITH_SAVEPOINT_NO_DRAIN";
            default:
                return "UNKNOWN_FINISHING_REASON";
        }
    }

void OmniSourceStreamTask::init()
{
    OmniStreamTask::init();
}

void OmniSourceStreamTask::processInput(MailboxDefaultAction::Controller *controller)
{
    LOG("OmniSourceStreamTask::processInput")
    if (!dynamic_cast<StreamSource<omnistream::VectorBatch> *>(mainOperator_)) {
        throw std::runtime_error("mainOperator_ is not of type StreamSource<omnistream::VectorBatch>");
    }
    
    dynamic_cast<StreamSource<omnistream::VectorBatch> *>(mainOperator_)->run();
    // clean up resources or emit final watermark
    CompleteProcessing();

    mailboxProcessor_->suspend();
    LOG_INFO_IMP("Task : " << taskName_ << " suspended");
}

void OmniSourceStreamTask::CompleteProcessing()
{
    // so we need to call it here
    auto stopMode = FinishingReasonToStopMode(finishingReason);
    if (stopMode == StopMode::DRAIN) {
       // reserved for future bound input
    }
    EndData(stopMode);
}

void OmniSourceStreamTask::AdvanceToEndOfEventTime()
{
    operatorChain->GetMainOperatorOutput()->emitWatermark(new Watermark(LONG_MAX));
}

const std::string OmniSourceStreamTask::getName() const
{
    return std::string("OmniSourceStreamTask");
}

void OmniSourceStreamTask::cancel()
{
    OmniStreamTask::cancel();
    // avoid back pressure
    recordWriter_->cancel();
}
}
