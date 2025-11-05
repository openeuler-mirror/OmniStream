/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef MOCKOMNITASK_H
#define MOCKOMNITASK_H

#include "taskmanager/OmniTask.h"

namespace omnistream {
    class MockOmniTask : public OmniTask {

    public:
        std::shared_ptr<TaskStateManagerBridge>stateBridge;
            std::shared_ptr<OmniTaskBridge> omni_task_bridge;
            std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge;

        MockOmniTask(
            std::shared_ptr<TaskStateManagerBridge> stateBridge,
            std::shared_ptr<OmniTaskBridge> omni_task_bridge,
            std::shared_ptr<TaskOperatorEventGatewayBridge> taskOperatorEventGatewayBridge
            ): OmniTask(stateBridge, omni_task_bridge, taskOperatorEventGatewayBridge,nullptr), //
        stateBridge(stateBridge),
        omni_task_bridge(omni_task_bridge),
        taskOperatorEventGatewayBridge(taskOperatorEventGatewayBridge)
        {}


        void triggerCheckpointBarrierV1(
            long checkpointid,
            long checkpointtimestamp,
            std::string checkpoint_options_str)
        {
            std::cout<<"we are here at triggerCheckpointBarrierV1"<<std::endl;
            std::string checkpointMetaDataJson = to_string(checkpointid);
            std::string checkpointMetricsJson = to_string(checkpointtimestamp);
            std::string acknowledgedStateJson = checkpoint_options_str;
            std::string localStateJson = "localStateJson";
            this->stateBridge->ReportTaskStateSnapshots(checkpointMetaDataJson, checkpointMetricsJson, acknowledgedStateJson, localStateJson);
        }
    };
}

#endif // MOCKOMNITASK_H

