/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/17/25.
//

#ifndef OPERATORCHAINPOD_H
#define OPERATORCHAINPOD_H

#include "OperatorPOD.h"

namespace omnistream {
    class OperatorChainPOD {
    public:
        OperatorChainPOD() : operators() {}
        explicit OperatorChainPOD(const std::vector<OperatorPOD>& operators) : operators(operators) {}
        OperatorChainPOD(const OperatorChainPOD& other) = default;
        OperatorChainPOD& operator=(const OperatorChainPOD& other) = default;

        std::vector<OperatorPOD> getOperators() const { return operators; }
        void setOperators(const std::vector<OperatorPOD>& operators) { this->operators = operators; }

        std::string toString() const
        {
            std::string operatorsStr = "[";
            for (const auto& op : operators) {
                operatorsStr += op.toString() + ", ";
            }
            if (!operators.empty()) {
                operatorsStr.pop_back(); // Remove the trailing ", "
                operatorsStr.pop_back();
            }
            operatorsStr += "]";
            return "OperatorChainPOD{operators=" + operatorsStr + "}";
        }

        NLOHMANN_DEFINE_TYPE_INTRUSIVE(OperatorChainPOD, operators)
    private:
        std::vector<OperatorPOD> operators;
    };

} // namespace omnistream


#endif //OPERATORCHAINPOD_H
