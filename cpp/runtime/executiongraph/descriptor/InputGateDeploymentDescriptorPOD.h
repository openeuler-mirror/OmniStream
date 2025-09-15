/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef INPUT_GATE_DEPLOYMENT_DESCRIPTOR_POD_H
#define INPUT_GATE_DEPLOYMENT_DESCRIPTOR_POD_H

#include <vector>
#include <string>
#include <executiongraph/common/IntermediateDataSetIDPOD.h>

#include "ShuffleDescriptorPOD.h"
#include "nlohmann/json.hpp"

namespace omnistream {

    class InputGateDeploymentDescriptorPOD {
    public:
        IntermediateDataSetIDPOD consumedResultId;
        int consumedPartitionType;
        int consumedSubpartitionIndex;
        std::vector<ShuffleDescriptorPOD> inputChannels;

        InputGateDeploymentDescriptorPOD():consumedPartitionType(0),consumedSubpartitionIndex(0){};
        InputGateDeploymentDescriptorPOD(IntermediateDataSetIDPOD consumedResultId, int consumedPartitionType, int consumedSubpartitionIndex, const std::vector<ShuffleDescriptorPOD>& inputChannels):
                consumedResultId(consumedResultId),consumedPartitionType(consumedPartitionType),consumedSubpartitionIndex(consumedSubpartitionIndex),inputChannels(inputChannels){};
        InputGateDeploymentDescriptorPOD(const InputGateDeploymentDescriptorPOD& other) = default;

        friend bool operator==(const InputGateDeploymentDescriptorPOD& lhs, const InputGateDeploymentDescriptorPOD& rhs)
        {
            return lhs.consumedResultId == rhs.consumedResultId
                && lhs.consumedPartitionType == rhs.consumedPartitionType
                && lhs.consumedSubpartitionIndex == rhs.consumedSubpartitionIndex
                && lhs.inputChannels == rhs.inputChannels;
        }

        friend bool operator!=(const InputGateDeploymentDescriptorPOD& lhs, const InputGateDeploymentDescriptorPOD& rhs)
        {
            return !(lhs == rhs);
        }

        friend std::size_t hash_value(const InputGateDeploymentDescriptorPOD& obj)
        {
            std::size_t seed = 0x5829DCAF;
            seed ^= (seed << 6) + (seed >> 2) + 0x3CB80869 + hash_value(obj.consumedResultId);
            seed ^= (seed << 6) + (seed >> 2) + 0x364BD974 + static_cast<std::size_t>(obj.consumedPartitionType);
            seed ^= (seed << 6) + (seed >> 2) + 0x48D85043 + static_cast<std::size_t>(obj.consumedSubpartitionIndex);

            seed ^= (seed << 6) + (seed >> 2) + 0x65AFCBFA;
            for (auto& item: obj.inputChannels)
            {
               seed ^= hash_value(item) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
            return seed;
        }

        //Getters
        IntermediateDataSetIDPOD getConsumedResultId() const { return consumedResultId; }
        int getConsumedPartitionType() const { return consumedPartitionType; }
        int getConsumedSubpartitionIndex() const { return consumedSubpartitionIndex; }
        const std::vector<ShuffleDescriptorPOD> &getShuffleDescriptors() const { return inputChannels; }

        //Setter
        void setConsumedResultId(const IntermediateDataSetIDPOD& consumedResultId){ this->consumedResultId = consumedResultId;}
        void setConsumedPartitionType(int consumedPartitionType){ this->consumedPartitionType = consumedPartitionType;}
        void setConsumedSubpartitionIndex(int consumedSubpartitionIndex){ this->consumedSubpartitionIndex = consumedSubpartitionIndex;}
        void setInputChannels(const std::vector<ShuffleDescriptorPOD>& inputChannels){ this->inputChannels = inputChannels;}

        std::string toString() const
        {
            //todo add str
            return "InputGateDeploymentDescriptorPOD";
        }

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(InputGateDeploymentDescriptorPOD, consumedResultId, consumedPartitionType, consumedSubpartitionIndex, inputChannels)
    };

} // namespace omnistream

namespace std
{
    template <>
    struct hash<omnistream::InputGateDeploymentDescriptorPOD> {
        std::size_t operator()(const omnistream::InputGateDeploymentDescriptorPOD& obj) const
        {
            return hash_value(obj);
        }
    };
}


#endif // INPUT_GATE_DEPLOYMENT_DESCRIPTOR_POD_H