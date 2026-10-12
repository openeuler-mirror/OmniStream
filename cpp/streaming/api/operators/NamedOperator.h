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

#ifndef FLINK_TNEL_NAMEDOPERATOR_H
#define FLINK_TNEL_NAMEDOPERATOR_H

#include <string>

// Non-templated mixin that carries an operator's own name (matches
// OperatorPOD::getName()). It deliberately does NOT inherit from StreamOperator,
// so concrete operators get exactly one NamedOperator sub-object (no diamond).
// This lets OperatorChain reach the name via a single dynamic_cast<NamedOperator*>
// without enumerating every AbstractStreamOperator<K> template instantiation.
class NamedOperator {
public:
    virtual ~NamedOperator() = default;

    virtual void SetOpName(const std::string& operatorName)
    {
        this->opName = operatorName;
    }

    virtual std::string GetOpName() const
    {
        return this->opName;
    }

protected:
    std::string opName;
};

#endif // FLINK_TNEL_NAMEDOPERATOR_H
