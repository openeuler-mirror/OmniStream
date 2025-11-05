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
#ifndef CPP_RUNTIME_JOBGRAPH_OPERATORID_H
#define CPP_RUNTIME_JOBGRAPH_OPERATORID_H
#include "runtime/executiongraph/common/AbstractIDPOD.h"
#include "JobVertexID.h"

using namespace omnistream;
class OperatorID : public AbstractIDPOD {
public:
    OperatorID() : AbstractIDPOD() {}

    OperatorID(const std::vector<uint8_t> &bytes)
        : AbstractIDPOD(bytes) {}

    OperatorID(uint64_t upperPart, uint64_t lowerPart)
        : AbstractIDPOD(upperPart, lowerPart) {}

    static OperatorID fromJobVertexID(const JobVertexID&id)
    {
        return OperatorID(id.getUpperPart(), id.getLowerPart());
    }
};

namespace std {
    template <>
    struct hash<OperatorID> {
        std::size_t operator()(const OperatorID& id) const noexcept
        {
            return hash_value(static_cast<const omnistream::AbstractIDPOD&>(id));
        }
    };
}
#endif // CPP_RUNTIME_JOBGRAPH_OPERATORID_H