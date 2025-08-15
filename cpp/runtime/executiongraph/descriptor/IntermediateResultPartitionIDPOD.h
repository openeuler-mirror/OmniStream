/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/12/25.
//

// IntermediateResultPartitionIDPOD.h
#ifndef INTERMEDIATERESULT_PARTITIONID_POD_H
#define INTERMEDIATERESULT_PARTITIONID_POD_H


#include <string>
#include <llvm/ADT/Hashing.h>

#include "../common/AbstractIDPOD.h" // Assuming AbstractIDPOD is defined in AbstractIDPOD.h
#include "nlohmann/json.hpp" // Include for JSON serialization

namespace omnistream {

    class IntermediateResultPartitionIDPOD {
    public:
        IntermediateResultPartitionIDPOD() : partitionNum(0) {}

        IntermediateResultPartitionIDPOD(const AbstractIDPOD& intermediateDataSetID, int partitionNum)
            : intermediateDataSetID(intermediateDataSetID), partitionNum(partitionNum) {}

        IntermediateResultPartitionIDPOD(const IntermediateResultPartitionIDPOD& other)
            : intermediateDataSetID(other.intermediateDataSetID),
              partitionNum(other.partitionNum)
        {
        }

        IntermediateResultPartitionIDPOD(IntermediateResultPartitionIDPOD&& other) noexcept
            : intermediateDataSetID(std::move(other.intermediateDataSetID)),
              partitionNum(other.partitionNum)
        {
        }

        IntermediateResultPartitionIDPOD& operator=(const IntermediateResultPartitionIDPOD& other)
        {
            if (this == &other) {
                return *this;
            }
            intermediateDataSetID = other.intermediateDataSetID;
            partitionNum = other.partitionNum;
            return *this;
        }

        IntermediateResultPartitionIDPOD& operator=(IntermediateResultPartitionIDPOD&& other) noexcept
        {
            if (this == &other) {
                return *this;
            }
            intermediateDataSetID = std::move(other.intermediateDataSetID);
            partitionNum = other.partitionNum;
            return *this;
        }

        const AbstractIDPOD& getIntermediateDataSetID() const { return intermediateDataSetID; }
        void setIntermediateDataSetID(const AbstractIDPOD& intermediateDataSetID) { this->intermediateDataSetID = intermediateDataSetID; }

        int getPartitionNum() const { return partitionNum; }
        void setPartitionNum(int partitionNum) { this->partitionNum = partitionNum; }

        std::string toString() const
        {
            return "{ intermediateDataSetID: " + intermediateDataSetID.toString() +
                   ", partitionNum: " + std::to_string(partitionNum) +
                   "}";
        }

        friend bool operator==(const IntermediateResultPartitionIDPOD& lhs, const IntermediateResultPartitionIDPOD& rhs)
        {
            return lhs.intermediateDataSetID == rhs.intermediateDataSetID
                && lhs.partitionNum == rhs.partitionNum;
        }

        friend bool operator!=(const IntermediateResultPartitionIDPOD& lhs, const IntermediateResultPartitionIDPOD& rhs)
        {
            return !(lhs == rhs);
        }

        friend std::size_t hash_value(const IntermediateResultPartitionIDPOD& obj)
        {
            std::size_t seed = 0x0633D95C;
            seed ^= (seed << 6) + (seed >> 2) + 0x64C6DE55 + hash_value(obj.intermediateDataSetID);
            seed ^= (seed << 6) + (seed >> 2) + 0x616123C0 + static_cast<std::size_t>(obj.partitionNum);
            return seed;
        }

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(IntermediateResultPartitionIDPOD, intermediateDataSetID, partitionNum)
    private:
        AbstractIDPOD intermediateDataSetID;
        int partitionNum;
    };

} // namespace omnistream

namespace std {
    template <>
    struct hash<omnistream::IntermediateResultPartitionIDPOD> {
        size_t operator()(const omnistream::IntermediateResultPartitionIDPOD& key) const
        {
            return hash_value(key);
        }
    };
}

#endif // INTERMEDIATERESULTPARTITIONIDPOD_H

