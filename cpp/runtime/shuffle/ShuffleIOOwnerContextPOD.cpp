/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "ShuffleIOOwnerContextPOD.h"

namespace omnistream {

    ShuffleIOOwnerContextPOD::ShuffleIOOwnerContextPOD() : ownerName(""), executionAttemptID() {}

    ShuffleIOOwnerContextPOD::ShuffleIOOwnerContextPOD(const std::string& ownerName, const ExecutionAttemptIDPOD& executionAttemptID)
        : ownerName(ownerName), executionAttemptID(executionAttemptID) {}

    ShuffleIOOwnerContextPOD::ShuffleIOOwnerContextPOD(const ShuffleIOOwnerContextPOD& other)
        : ownerName(other.ownerName), executionAttemptID(other.executionAttemptID) {}

    ShuffleIOOwnerContextPOD::~ShuffleIOOwnerContextPOD() {}

    std::string ShuffleIOOwnerContextPOD::getOwnerName() const
    {
        return ownerName;
    }


    void ShuffleIOOwnerContextPOD::setOwnerName(const std::string& ownerName)
    {
        this->ownerName = ownerName;
    }

    ExecutionAttemptIDPOD ShuffleIOOwnerContextPOD::getExecutionAttemptID() const
    {
        return executionAttemptID;
    }

    void ShuffleIOOwnerContextPOD::setExecutionAttemptID(const ExecutionAttemptIDPOD& executionAttemptID)
    {
        this->executionAttemptID = executionAttemptID;
    }

    std::string ShuffleIOOwnerContextPOD::toString() const
    {
        std::stringstream ss;
        ss << "ShuffleIOOwnerContextPOD{ownerName='" << ownerName << "', executionAttemptID=" << executionAttemptID.toString() << "}";
        return ss.str();
    }

} // namespace omnistream