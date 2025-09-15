/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include <utility>
#include "executiongraph/common/IntermediateDataSetIDPOD.h"
#include "executiongraph/descriptor/ExecutionAttemptIDPOD.h"

#ifndef OMNISTREAM_INPUTGATEID_H
#define OMNISTREAM_INPUTGATEID_H


namespace omnistream {
    class InputGateID {
    private:
        IntermediateDataSetIDPOD consumedResultID;
        ExecutionAttemptIDPOD consumerID;

    public:
        InputGateID(IntermediateDataSetIDPOD  consumedResultID, ExecutionAttemptIDPOD consumerID)
            : consumedResultID(std::move(consumedResultID)), consumerID(std::move(consumerID)) {}

        IntermediateDataSetIDPOD getConsumedResultID()  {
            return consumedResultID;
        }

        ExecutionAttemptIDPOD getConsumerID() {
            return consumerID;
        }

        bool operator==(const InputGateID& other) const {
            return consumedResultID == other.consumedResultID &&
                   consumerID == other.consumerID;
        }

        bool operator!=(const InputGateID& other) const {
            return !(*this == other);
        }

        bool operator<(const InputGateID& other) const {
            return true;
        }

        size_t hashCode() const {
            return std::hash<std::size_t>()(hash_value(consumedResultID)) ^ std::hash<std::size_t>()(hash_value(consumerID));
        }

        std::string toString() const {
            return consumedResultID.toString() + "@" + consumerID.toString();
        }
    };
}


#endif //OMNISTREAM_INPUTGATEID_H
