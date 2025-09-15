/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef TASKDEPLOYMENTDESCRIPTORPOD_H
#define TASKDEPLOYMENTDESCRIPTORPOD_H


#include <nlohmann/json.hpp>
#include <vector>
#include <executiongraph/JobIDPOD.h>
#include "ResultPartitionDeploymentDescriptorPOD.h"
#include "InputGateDeploymentDescriptorPOD.h"

namespace omnistream {

    class TaskDeploymentDescriptorPOD {
    public:
        // Default constructor
        TaskDeploymentDescriptorPOD() = default;

        // Full argument constructor
        TaskDeploymentDescriptorPOD(const JobIDPOD& jobId,
                                     const std::vector<ResultPartitionDeploymentDescriptorPOD>& producedPartitions,
                                     const std::vector<InputGateDeploymentDescriptorPOD>& inputGates)
            : jobId(jobId), producedPartitions(producedPartitions), inputGates(inputGates) {}

        // Copy constructor
        TaskDeploymentDescriptorPOD(const TaskDeploymentDescriptorPOD& other) = default;

        // Getters
        const JobIDPOD& getJobId() const { return jobId; }
        const std::vector<ResultPartitionDeploymentDescriptorPOD>& getProducedPartitions() const { return producedPartitions; }
        const std::vector<InputGateDeploymentDescriptorPOD>& getInputGates() const { return inputGates; }

        // Setters
        void setJobId(const JobIDPOD& jobId) { this->jobId = jobId; }
        void setProducedPartitions(const std::vector<ResultPartitionDeploymentDescriptorPOD>& producedPartitions) { this->producedPartitions = producedPartitions; }
        void setInputGates(const std::vector<InputGateDeploymentDescriptorPOD>& inputGates) { this->inputGates = inputGates; }

        // toString method
        std::string toString() const
        {
            std::string str = "TaskDeploymentDescriptorPOD:\n";
            str += "  JobID: " + jobId.toString() + "\n";
            str += "  Produced Partitions:\n";
            for (const auto& partition : producedPartitions) {
                str += "    " + partition.toString() + "\n";
            }
            str += "  Input Gates:\n";
            for (const auto& gate : inputGates) {
                str += "    " + gate.toString() + "\n";
            }
            return str;
        }

        // Equality operator
        bool operator==(const TaskDeploymentDescriptorPOD& other) const
        {
            return jobId == other.jobId && producedPartitions == other.producedPartitions && inputGates == other.inputGates;
        }

        // Hash function
        size_t hash() const
        {
            size_t h1 = hash_value(jobId);
            size_t h2 = 0;
            for (const auto& partition : producedPartitions) {
                h2 ^= hash_value(partition);
            }
            size_t h3 = 0;
            for(const auto& gate: inputGates) {
                h3^= hash_value(gate);
            }

            return h1 ^ (h2 << 1) ^ (h3 << 2);
        }

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(TaskDeploymentDescriptorPOD, jobId, producedPartitions, inputGates)
    private:
        JobIDPOD jobId;
        std::vector<ResultPartitionDeploymentDescriptorPOD> producedPartitions;
        std::vector<InputGateDeploymentDescriptorPOD> inputGates;
    };

} // namespace omnistreamcd

namespace std {
    template <>
    struct hash<omnistream::TaskDeploymentDescriptorPOD> {
        std::size_t operator()(const omnistream::TaskDeploymentDescriptorPOD& obj) const
        {
            return obj.hash();
        }
    };
}


#endif // TASKDEPLOYMENTDESCRIPTORPOD_H