/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNIRUNTIMEENVIRONMENT_V2_H
#define OMNIRUNTIMEENVIRONMENT_V2_H


#include <memory>
#include <executiongraph/JobInformationPOD.h>
#include <executiongraph/TaskInformationPOD.h>
#include <executiongraph/common/TaskPlainInfoPOD.h>
#include <partition/ResultPartitionWriter.h>
#include <sstream>


#include "runtime/execution/OmniEnvironment.h"
#include "runtime/shuffle/ShuffleEnvironment.h"
#include "metrics/groups/TaskMetricGroup.h"

namespace omnistream {
    class OmniTask;

    class RuntimeEnvironmentV2 : public EnvironmentV2 {
    public:
        ~RuntimeEnvironmentV2();
        RuntimeEnvironmentV2() = default;
        RuntimeEnvironmentV2(const std::shared_ptr<ShuffleEnvironment>& omniShuffleEnvironment,
            const TaskInformationPOD& taskConfiguration, const JobInformationPOD& jobConfiguration,
            const TaskPlainInfoPOD& taskPlainInfoPod,
            const std::vector<std::shared_ptr<ResultPartitionWriter>>& writers,
            const std::vector<std::shared_ptr<InputGate>>& inputGates,  std::shared_ptr<OmniTask> omniTask,
            std::shared_ptr<TaskMetricGroup> &taskMetricGroup) : omniShuffleEnvironment_(omniShuffleEnvironment),
            taskConfiguration_(taskConfiguration),
            jobConfiguration_(jobConfiguration),
            taskPlainInfoPod_(taskPlainInfoPod),
            writers_(writers),
            inputGates(inputGates),
            omniTask_(omniTask),
            taskMetricGroup_(taskMetricGroup)
        {
        }

        [[nodiscard]] std::shared_ptr<ShuffleEnvironment> omni_shuffle_environment() const
        {
            return omniShuffleEnvironment_;
        }

        [[nodiscard]] TaskInformationPOD taskConfiguration() const
        {
            return taskConfiguration_;
        }

        void setTaskConfiguration(TaskInformationPOD taskConfiguration)
        {
            taskConfiguration_ = taskConfiguration;
        }

        [[nodiscard]] JobInformationPOD jobConfiguration() const
        {
            return jobConfiguration_;
        }

        [[nodiscard]] std::shared_ptr<TaskMetricGroup> taskMetricGroup() const
        {
            return taskMetricGroup_;
        }

        [[nodiscard]] TaskPlainInfoPOD taskPlainInfo() const
        {
            return taskPlainInfoPod_;
        }

        [[nodiscard]] std::vector<std::shared_ptr<ResultPartitionWriter>> writers() const
        {
            return writers_;
        }

        [[nodiscard]] std::vector<std::shared_ptr<InputGate>> inputGates1() const
        {
            return inputGates;
        }

        [[nodiscard]] std::shared_ptr<OmniTask> omniTask() const
        {
            return omniTask_;
        }

        // Visibility helper
        std::string toString() const
        {
            std::stringstream ss;
            ss << "RuntimeEnvironment {" << std::endl;

            ss << "  omniShuffleEnvironment_: ";
            if (omniShuffleEnvironment_) {
                ss << "shared_ptr (non-null)"
                   << std::endl; // You might want to add more details about the ShuffleEnvironment if possible.
            } else {
                ss << "nullptr" << std::endl;
            }
            ss << "  taskConfiguration_: " << taskConfiguration_.toString()
               << std::endl; // Assuming TaskInformationPOD has a toString()
            ss << "  jobConfiguration_: " << jobConfiguration_.toString()
               << std::endl; // Assuming JobInformationPOD has a toString()
            ss << "  taskPlainInfoPod_: " << taskPlainInfoPod_.toString()
               << std::endl; // Assuming TaskPlainInfoPOD has a toString()

            ss << "  writers_: [";
            ss << "]" << std::endl;

            ss << "}";
            return ss.str();
        }
    private:
        std::shared_ptr<ShuffleEnvironment> omniShuffleEnvironment_;
        TaskInformationPOD taskConfiguration_;
        JobInformationPOD jobConfiguration_;
        TaskPlainInfoPOD taskPlainInfoPod_;
        /**
        *  private final TaskInfo taskInfo;
         */
        std::vector<std::shared_ptr<ResultPartitionWriter>> writers_;
        std::vector<std::shared_ptr<InputGate>> inputGates;
        std::shared_ptr<OmniTask> omniTask_;
        std::shared_ptr<TaskMetricGroup> taskMetricGroup_;
        // IndexedInputGate[] inputGates;
    };
}
#endif  //OMNIRUNTIMEENVIRONMENT_H
