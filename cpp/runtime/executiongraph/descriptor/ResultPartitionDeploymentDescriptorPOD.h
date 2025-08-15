/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/12/25.
//

#ifndef RESULTPARTITIONDEPLOYMENTDESCRIPTORPOD_H
#define RESULTPARTITIONDEPLOYMENTDESCRIPTORPOD_H


#include <string>
#include <iostream>
#include "PartitionDescriptorPOD.h"
#include "ShuffleDescriptorPOD.h"
#include "nlohmann/json.hpp"

namespace omnistream {

    class ResultPartitionDeploymentDescriptorPOD {
    public:
    // Default Constructor
    ResultPartitionDeploymentDescriptorPOD() : maxParallelism(0), notifyPartitionDataAvailable(false), totalNumberOfPartitions(0), partitionType(0), numberOfSubpartitions(0) {}

    // Full Argument Constructor
    ResultPartitionDeploymentDescriptorPOD(const ShuffleDescriptorPOD& shuffleDescriptor, int maxParallelism, bool notifyPartitionDataAvailable, const AbstractIDPOD& resultId, int totalNumberOfPartitions, const IntermediateResultPartitionIDPOD& partitionId, int partitionType, int numberOfSubpartitions)
        : shuffleDescriptor(shuffleDescriptor), maxParallelism(maxParallelism), notifyPartitionDataAvailable(notifyPartitionDataAvailable), resultId(resultId), totalNumberOfPartitions(totalNumberOfPartitions), partitionId(partitionId), partitionType(partitionType), numberOfSubpartitions(numberOfSubpartitions) {}

    // Copy Constructor
    ResultPartitionDeploymentDescriptorPOD(const ResultPartitionDeploymentDescriptorPOD& other) = default;

    friend bool operator==(const ResultPartitionDeploymentDescriptorPOD& lhs,
        const ResultPartitionDeploymentDescriptorPOD& rhs)
    {
        return lhs.shuffleDescriptor == rhs.shuffleDescriptor
            && lhs.maxParallelism == rhs.maxParallelism
            && lhs.notifyPartitionDataAvailable == rhs.notifyPartitionDataAvailable
            && lhs.resultId == rhs.resultId
            && lhs.totalNumberOfPartitions == rhs.totalNumberOfPartitions
            && lhs.partitionId == rhs.partitionId
            && lhs.partitionType == rhs.partitionType
            && lhs.numberOfSubpartitions == rhs.numberOfSubpartitions;
    }

    friend bool operator!=(const ResultPartitionDeploymentDescriptorPOD& lhs,
        const ResultPartitionDeploymentDescriptorPOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const ResultPartitionDeploymentDescriptorPOD& obj)
    {
        std::size_t seed = 0x155BBB92;
        seed ^= (seed << 6) + (seed >> 2) + 0x58A6D5E0 + hash_value(obj.shuffleDescriptor);
        seed ^= (seed << 6) + (seed >> 2) + 0x215E2E58 + static_cast<std::size_t>(obj.maxParallelism);
        seed ^= (seed << 6) + (seed >> 2) + 0x21D75AAE + static_cast<std::size_t>(obj.notifyPartitionDataAvailable);
        seed ^= (seed << 6) + (seed >> 2) + 0x39D39059 + hash_value(obj.resultId);
        seed ^= (seed << 6) + (seed >> 2) + 0x5E8DAF5F + static_cast<std::size_t>(obj.totalNumberOfPartitions);
        seed ^= (seed << 6) + (seed >> 2) + 0x74EDA8D7 + hash_value(obj.partitionId);
        seed ^= (seed << 6) + (seed >> 2) + 0x408D1540 + static_cast<std::size_t>(obj.partitionType);
        seed ^= (seed << 6) + (seed >> 2) + 0x66B6E21F + static_cast<std::size_t>(obj.numberOfSubpartitions);
        return seed;
    }

    // Getters
    const ShuffleDescriptorPOD& getShuffleDescriptor() const { return shuffleDescriptor; }
    int getMaxParallelism() const { return maxParallelism; }
    bool isNotifyPartitionDataAvailable() const { return notifyPartitionDataAvailable; }
    const AbstractIDPOD& getResultId() const { return resultId; }
    int getTotalNumberOfPartitions() const { return totalNumberOfPartitions; }
    const IntermediateResultPartitionIDPOD& getPartitionId() const { return partitionId; }
    int getPartitionType() const { return partitionType; }
    int getNumberOfSubpartitions() const { return numberOfSubpartitions; }

    // Setters
    void setShuffleDescriptor(const ShuffleDescriptorPOD& shuffleDescriptor) { this->shuffleDescriptor = shuffleDescriptor; }
    void setMaxParallelism(int maxParallelism) { this->maxParallelism = maxParallelism; }
    void setNotifyPartitionDataAvailable(bool notifyPartitionDataAvailable) { this->notifyPartitionDataAvailable = notifyPartitionDataAvailable; }
    void setResultId(const AbstractIDPOD& resultId) { this->resultId = resultId; }
    void setTotalNumberOfPartitions(int totalNumberOfPartitions) { this->totalNumberOfPartitions = totalNumberOfPartitions; }
    void setPartitionId(const IntermediateResultPartitionIDPOD& partitionId) { this->partitionId = partitionId; }
    void setPartitionType(int partitionType) { this->partitionType = partitionType; }
    void setNumberOfSubpartitions(int numberOfSubpartitions) { this->numberOfSubpartitions = numberOfSubpartitions; }

    // toString() method
    std::string toString() const
    {
        return "ResultPartitionDeploymentDescriptorPOD{"
               "shuffleDescriptor=" + shuffleDescriptor.toString() + // Assuming toString() exists in ShuffleDescriptorPOD
               ", maxParallelism=" + std::to_string(maxParallelism) +
               ", notifyPartitionDataAvailable=" + std::to_string(notifyPartitionDataAvailable) +
               ", resultId=" + resultId.toString() + // Assuming toString() exists in AbstractIDPOD
               ", totalNumberOfPartitions=" + std::to_string(totalNumberOfPartitions) +
               ", partitionId=" + partitionId.toString() + // Assuming toString() exists in IntermediateResultPartitionIDPOD
               ", partitionType=" + std::to_string(partitionType) +
               ", numberOfSubpartitions=" + std::to_string(numberOfSubpartitions) +
               "}";
    }

    // JSON Serialization/Deserialization
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ResultPartitionDeploymentDescriptorPOD, shuffleDescriptor, maxParallelism, notifyPartitionDataAvailable, resultId, totalNumberOfPartitions, partitionId, partitionType, numberOfSubpartitions)
    private:
    ShuffleDescriptorPOD shuffleDescriptor;
    int maxParallelism;
    bool notifyPartitionDataAvailable;

        //PartitionDescriptorPOD partitionDescriptor;
    AbstractIDPOD resultId;
    int totalNumberOfPartitions;
    IntermediateResultPartitionIDPOD partitionId;
    int partitionType;
    int numberOfSubpartitions;
};

} // namespace omnistream

namespace std {
    template <>
    struct hash<omnistream::ResultPartitionDeploymentDescriptorPOD> {
        std::size_t operator()(const omnistream::ResultPartitionDeploymentDescriptorPOD& obj) const
        {
            return hash_value(obj);
        }
    };
}


#endif