/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/29/25.
//

#include "OmniShuffleEnvironment.h"

#include <utility>

namespace omnistream {
    ShuffleIOOwnerContextPOD OmniShuffleEnvironment::createShuffleIOOwnerContext(
        const std::string& ownerName, const ExecutionAttemptIDPOD& executionAttemptID,
        std::shared_ptr<MetricGroup> parentGroup)
    {
        ShuffleIOOwnerContextPOD context(ownerName, executionAttemptID);
        return context;
    }

    std::vector<std::shared_ptr<ResultPartitionWriter>> OmniShuffleEnvironment::createResultPartitionWriters(
        ShuffleIOOwnerContextPOD ownerContext,
        const std::vector<ResultPartitionDeploymentDescriptorPOD>&
        resultPartitionDeploymentDescriptors)
    {
        std::lock_guard<std::mutex> lockGuard(lock);

        if (isClosed_) {
            THROW_LOGIC_EXCEPTION("The OmniShuffleEnvironment has already been shut down.")
        }

        LOG_INFO_IMP("Before create writer")
        std::vector<std::shared_ptr<ResultPartitionWriter>> resultPartitions(resultPartitionDeploymentDescriptors.size());
        for (size_t partitionIndex = 0; partitionIndex < resultPartitions.size(); ++partitionIndex) {
            LOG("Creating partition " << std::to_string(partitionIndex)  <<
                " partition descriptor: " << resultPartitionDeploymentDescriptors[partitionIndex] . toString())
            // todo: pass the parameter config to the create function
            resultPartitions[partitionIndex] = resultPartitionFactory->create(
                ownerContext.getOwnerName(),
                partitionIndex,
                resultPartitionDeploymentDescriptors[partitionIndex],
                // todo:add config
                config);
        }
        LOG_INFO_IMP("Before return  writer")
        return resultPartitions;
    }

    std::vector<std::shared_ptr<SingleInputGate>>
    OmniShuffleEnvironment::createInputGates(ShuffleIOOwnerContextPOD ownerContext,
                                             std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
                                             const std::vector<InputGateDeploymentDescriptorPOD> &inputGateDeploymentDescriptors) {
        std::lock_guard<std::mutex> lockGuard(lock);
        if (isClosed_) {
            THROW_LOGIC_EXCEPTION("The OmniShuffleEnvironment has already been shut down.")
        }
        //todo add Metrics
        //InputChannelMetrics inputChannelMetrics =
        std::vector<std::shared_ptr<SingleInputGate>> inputGates;
        for (size_t gateIndex = 0; gateIndex < inputGateDeploymentDescriptors.size(); ++gateIndex) {
            const auto& igdd = inputGateDeploymentDescriptors[gateIndex];
            auto inputGate = singleInputGateFactory->create(ownerContext.getOwnerName(), gateIndex,
                                                            std::make_shared<InputGateDeploymentDescriptorPOD>(igdd), partitionProducerStateProvider);
            LOG("InputGate created successfully.")
            auto *id = new InputGateID(igdd.getConsumedResultId(), ownerContext.getExecutionAttemptID());
            (*inputGatesById)[*id] = inputGate;

            class RemoveIdTask : public Runnable {
            public:
                std::shared_ptr<std::map<InputGateID, std::shared_ptr<SingleInputGate>>> map;
                std::shared_ptr<InputGateID> id;
                RemoveIdTask(std::shared_ptr<std::map<InputGateID, std::shared_ptr<SingleInputGate>>> map,
                             std::shared_ptr<InputGateID> id):map(std::move(map)),id(std::move(id)){}
                void run() override {
                    (*map).erase(*id);
                }
            };
            //todo set RemoveIdTask to CloseFuture
            LOG("InputGate CloseFuture set successfully")
            inputGates.push_back(inputGate);
        }

        return inputGates;
    };

}
