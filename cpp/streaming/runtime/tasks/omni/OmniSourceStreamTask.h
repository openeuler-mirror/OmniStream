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

#ifndef OMNISOURCESTREAMTASK_H
#define OMNISOURCESTREAMTASK_H
#include <taskmanager/OmniRuntimeEnvironment.h>

#include "OmniStreamTask.h"
#include "streaming/api/operators/StreamSource.h"
#include "io/network/api/StopMode.h"
#include <thread>
#include "OmniAsyncDataOutputToOutput.h"
#include <limits>


namespace omnistream {

    enum class FinishingReason : int {
        END_OF_DATA = 1,
        STOP_WITH_SAVEPOINT_DRAIN,
        STOP_WITH_SAVEPOINT_NO_DRAIN
    };

    StopMode FinishingReasonToStopMode(FinishingReason reason);

    // Optional: For easier debugging or logging
    std::string FinishingReasonToString(FinishingReason reason);


    class OmniSourceStreamTask : public OmniStreamTask {
    public:
        explicit OmniSourceStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env, int taskType)
            : OmniStreamTask(env, taskType) {
        }

        void init() override;

        void processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller) override;

        ~OmniSourceStreamTask() override = default;
        const std::string getName() const override;
        void AdvanceToEndOfEventTime() override;
        void cancel() override;

    private:
        FinishingReason finishingReason = FinishingReason::END_OF_DATA;
        void CompleteProcessing();
        std::shared_ptr<OmniAsyncDataOutputToOutput> output;
    };
}


#endif // OMNISOURCESTREAMTASK_H
