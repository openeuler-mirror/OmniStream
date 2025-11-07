/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef INPUTGATEID_H
#define INPUTGATEID_H

#include <utility>
#include "executiongraph/common/IntermediateDataSetIDPOD.h"
#include "executiongraph/descriptor/ExecutionAttemptIDPOD.h"


#ifndef OMNISTREAM_INPUTGATEID_H
#define OMNISTREAM_INPUTGATEID_H


namespace omnistream {
    class InputGateID {
    public:
        InputGateID(IntermediateDataSetIDPOD  consumedResultID, ExecutionAttemptIDPOD consumerID)
            : consumedResultID(std::move(consumedResultID)), consumerID(std::move(consumerID)) {}

        IntermediateDataSetIDPOD getConsumedResultID()
        {
            return consumedResultID;
        }

        ExecutionAttemptIDPOD getConsumerID()
        {
            return consumerID;
        }

        bool operator==(const InputGateID& other) const
        {
            return consumedResultID == other.consumedResultID &&
                   consumerID == other.consumerID;
        }

        bool operator!=(const InputGateID& other) const
        {
            return !(*this == other);
        }

        bool operator<(const InputGateID& other) const
        {
            return consumedResultID < other.consumedResultID;
        }

        size_t hashCode() const
        {
            return std::hash<std::size_t>()(hash_value(consumedResultID)) ^ std::hash<std::size_t>()(hash_value(consumerID));
        }

        std::string toString() const
        {
            return consumedResultID.toString() + "@" + consumerID.toString();
        }
    private:
        IntermediateDataSetIDPOD consumedResultID;
        ExecutionAttemptIDPOD consumerID;
    };
}


#endif

#endif
