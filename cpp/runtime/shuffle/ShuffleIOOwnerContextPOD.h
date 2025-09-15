/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef SHUFFLEIOOWNERCONTEXTPOD_H
#define SHUFFLEIOOWNERCONTEXTPOD_H


#include <string>
#include <memory>
#include <sstream>
#include <executiongraph/descriptor/ExecutionAttemptIDPOD.h>

namespace omnistream
{
    class ShuffleIOOwnerContextPOD
    {
    public:
        ShuffleIOOwnerContextPOD();
        ShuffleIOOwnerContextPOD(const std::string& ownerName, const ExecutionAttemptIDPOD& executionAttemptID);
        ShuffleIOOwnerContextPOD(const ShuffleIOOwnerContextPOD& other);
        ~ShuffleIOOwnerContextPOD();

        std::string getOwnerName() const;
        void setOwnerName(const std::string& ownerName);
        ExecutionAttemptIDPOD getExecutionAttemptID() const;
        void setExecutionAttemptID(const ExecutionAttemptIDPOD& executionAttemptID);

        bool operator==(const ShuffleIOOwnerContextPOD& other) const {
            return ownerName == other.getOwnerName() && executionAttemptID == other.getExecutionAttemptID();
        }

        std::string toString() const;
    private:
        std::string ownerName;
        ExecutionAttemptIDPOD executionAttemptID;

    };
}

namespace std {
    template <>
    struct hash<omnistream::ShuffleIOOwnerContextPOD> {
        std::size_t operator()(const omnistream::ShuffleIOOwnerContextPOD& obj) const {
            size_t h1= std::hash<std::string>{}(obj.getOwnerName());
            size_t h2=hash_value(obj.getExecutionAttemptID());
            return h1 ^ (h2 << 1);
        }
    };
}



#endif //SHUFFLEIOOWNERCONTEXTPOD_H
