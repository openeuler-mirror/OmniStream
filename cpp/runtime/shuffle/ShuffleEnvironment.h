/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/8/25.
//

// ShuffleEnvironment.h
#ifndef SHUFFLEENVIRONMENT_H
#define SHUFFLEENVIRONMENT_H

#include <iostream>
#include <vector>
#include <memory>
#include <string>
#include <set>
#include <executiongraph/descriptor/ExecutionAttemptIDPOD.h>
#include <executiongraph/descriptor/InputGateDeploymentDescriptorPOD.h>
#include <executiongraph/descriptor/PartitionInfoPOD.h>
#include <executiongraph/descriptor/ResultPartitionDeploymentDescriptorPOD.h>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>
#include <metrics/MetricGroup.h>
#include <partition/ResultPartitionWriter.h>
#include <partition/consumer/IndexedInputGate.h>
#include <partition/PartitionProducerStateProvider.h>

#include "ShuffleIOOwnerContextPOD.h"

namespace omnistream
{
    class ShuffleEnvironment
    {
    public:
        virtual ~ShuffleEnvironment() = default;

        virtual int start() = 0;

        virtual ShuffleIOOwnerContextPOD createShuffleIOOwnerContext(
            const std::string& ownerName,
            const ExecutionAttemptIDPOD& executionAttemptID,
            std::shared_ptr<MetricGroup> parentGroup) = 0;

        virtual std::vector<std::shared_ptr<ResultPartitionWriter>> createResultPartitionWriters(
            ShuffleIOOwnerContextPOD ownerContext,
            const std::vector<ResultPartitionDeploymentDescriptorPOD>&
            resultPartitionDeploymentDescriptors) = 0;

        virtual void releasePartitionsLocally(const std::set<ResultPartitionIDPOD>& partitionIds) = 0;

        virtual std::set<ResultPartitionIDPOD> getPartitionsOccupyingLocalResources() = 0;

        virtual std::vector<std::shared_ptr<SingleInputGate>> createInputGates(
            ShuffleIOOwnerContextPOD ownerContext,
            std::shared_ptr<PartitionProducerStateProvider> partitionProducerStateProvider,
            const std::vector<InputGateDeploymentDescriptorPOD>& inputGateDeploymentDescriptors) = 0;

        virtual bool updatePartitionInfo(ExecutionAttemptIDPOD consumerID,
                                         PartitionInfoPOD partitionInfo) = 0;

        virtual void close() = 0;
    };
} // namespace omnistream

#endif // SHUFFLEENVIRONMENT_H
