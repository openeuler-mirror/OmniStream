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
#ifndef JOBVERTEXID_H
#define JOBVERTEXID_H
#include "runtime/executiongraph/common/AbstractIDPOD.h"

namespace omnistream {
    class JobVertexID : public AbstractIDPOD {
    public:
        
        JobVertexID() : AbstractIDPOD() {}

        JobVertexID(const std::vector<uint8_t>& bytes) : AbstractIDPOD(bytes) {}

        JobVertexID(long upperPart, long lowerPart): AbstractIDPOD(upperPart, lowerPart) {}
    };
}

namespace std {
    template<>
    struct hash<omnistream::JobVertexID> {
        size_t operator()(const omnistream::JobVertexID& id) const
        {
            return hash_value(static_cast<omnistream::AbstractIDPOD const&>(id));
        }
    };
}

#endif // JOBVERTEXID_H
