/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/12/25.
//

#ifndef RESULTPARTITIONIDPOD_H
#define RESULTPARTITIONIDPOD_H

#include <string>
#include <nlohmann/json.hpp>
#include "IntermediateResultPartitionIDPOD.h" // Include header for IntermediateResultPartitionIDPOD
#include "ExecutionAttemptIDPOD.h"        // Include header for ExecutionAttemptIDPOD

namespace omnistream {

struct ResultPartitionIDPOD {
public:
    // Default constructor
    ResultPartitionIDPOD() = default;
    ~ResultPartitionIDPOD() = default;
    // Full argument constructor
    ResultPartitionIDPOD(const IntermediateResultPartitionIDPOD& partitionId, const ExecutionAttemptIDPOD& producerId)
        : partitionId(partitionId), producerId(producerId) {}

    friend bool operator==(const ResultPartitionIDPOD& lhs, const ResultPartitionIDPOD& rhs)
    {
        return lhs.partitionId == rhs.partitionId
            && lhs.producerId == rhs.producerId;
    }

    friend bool operator!=(const ResultPartitionIDPOD& lhs, const ResultPartitionIDPOD& rhs)
    {
        return !(lhs == rhs);
    }

    friend std::size_t hash_value(const ResultPartitionIDPOD& obj)
    {
        std::size_t seed = 0x133AAB57;
        seed ^= (seed << 6) + (seed >> 2) + 0x3010BC7E + hash_value(obj.partitionId);
        seed ^= (seed << 6) + (seed >> 2) + 0x2244B1BF + hash_value(obj.producerId);
        return seed;
    }

    // Getters
    const IntermediateResultPartitionIDPOD& getPartitionId() const { return partitionId; }
    const ExecutionAttemptIDPOD& getProducerId() const { return producerId; }

    // Setters
    void setPartitionId(const IntermediateResultPartitionIDPOD& partitionId) { this->partitionId = partitionId; }
    void setProducerId(const ExecutionAttemptIDPOD& producerId) { this->producerId = producerId; }

    // toString method (example implementation)
    std::string toString() const
    {
        return "ResultPartitionIDPOD { "
               "partitionId: " + partitionId.toString() + ", "
               "producerId: " + producerId.toString() + " }";
    }

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ResultPartitionIDPOD, partitionId, producerId)
private:
    IntermediateResultPartitionIDPOD partitionId;
    ExecutionAttemptIDPOD producerId;
};

} // namespace omnistream

namespace std
{
    template <>
    struct hash<omnistream::ResultPartitionIDPOD> {
        std::size_t operator()(const omnistream::ResultPartitionIDPOD& obj) const
        {
            return hash_value(obj);
        }
    };
}

#endif // RESULT_PARTITION_ID_POD_H

