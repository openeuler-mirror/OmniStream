/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISOURCESTREAMTASK_H
#define OMNISOURCESTREAMTASK_H
#include <taskmanager/OmniRuntimeEnvironment.h>

#include "OmniStreamTask.h"
#include "core/operators/StreamSource.h"
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
        explicit OmniSourceStreamTask(std::shared_ptr<RuntimeEnvironmentV2> &env)
            : OmniStreamTask(env) {
        }

        void init() override;

        void processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller) override;

        ~OmniSourceStreamTask() override = default;
        const std::string getName() const override;
        void AdvanceToEndOfEventTime() override;

    private:
        FinishingReason finishingReason = FinishingReason::END_OF_DATA;
        void CompleteProcessing();
        std::shared_ptr<OmniAsyncDataOutputToOutput> output;
    };
}


#endif //OMNISOURCESTREAMTASK_H
