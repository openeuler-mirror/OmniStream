/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/12/25.
//

#ifndef OMNISTREAM_EXECUTIONATTEMPTIDPOD_H
#define OMNISTREAM_EXECUTIONATTEMPTIDPOD_H

#include <string>
#include <nlohmann/json.hpp>
#include "../common/AbstractIDPOD.h" // Assuming this header defines AbstractIDPOD

namespace omnistream {

    class ExecutionAttemptIDPOD {
    public:
        // Default constructor
        ExecutionAttemptIDPOD() = default;

        // Full argument constructor
        ExecutionAttemptIDPOD(const AbstractIDPOD &executionAttemptId) : executionAttemptId(executionAttemptId) {
        }

        friend bool operator==(const ExecutionAttemptIDPOD& lhs, const ExecutionAttemptIDPOD& rhs)
        {
            return lhs.executionAttemptId == rhs.executionAttemptId;
        }

        friend bool operator!=(const ExecutionAttemptIDPOD& lhs, const ExecutionAttemptIDPOD& rhs)
        {
            return !(lhs == rhs);
        }

        friend std::size_t hash_value(const ExecutionAttemptIDPOD& obj)
        {
            std::size_t seed = 0x2A9B61C8;
            seed ^= (seed << 6) + (seed >> 2) + 0x21D57D88 + hash_value(obj.executionAttemptId);
            return seed;
        }

        // Getter for executionAttemptId
        const AbstractIDPOD& getExecutionAttemptId() const { return executionAttemptId; }

        // Setter for executionAttemptId
        void setExecutionAttemptId(const AbstractIDPOD& executionAttemptId) { this->executionAttemptId = executionAttemptId; }

        // toString method
        std::string toString() const
        {
            return "{ executionAttemptId: " + executionAttemptId.toString() + " }";  // Assuming AbstractIDPOD has a toString()
        }

        // JSON serialization/deserialization using NLOHMANN_DEFINE_TYPE_INTRUSIVE
        NLOHMANN_DEFINE_TYPE_INTRUSIVE(ExecutionAttemptIDPOD, executionAttemptId)
    private:
        AbstractIDPOD executionAttemptId;
      };

} // namespace omnistream
namespace std {
    template <>
    struct hash<omnistream::ExecutionAttemptIDPOD> {
        std::size_t operator()(const omnistream::ExecutionAttemptIDPOD& obj) const
        {
            return hash_value(obj);
        }
    };
}

#endif // OMNISTREAM_EXECUTIONATTEMPTIDPOD_H
