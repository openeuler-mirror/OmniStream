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

#ifndef OMNISTREAM_EXECUTIONGRAPHIDPOD_H
#define OMNISTREAM_EXECUTIONGRAPHIDPOD_H

#include "runtime/executiongraph/common/AbstractIDPOD.h"

namespace omnistream {
    
    class ExecutionGraphIDPOD : public AbstractIDPOD {
    public:
        ExecutionGraphIDPOD() : AbstractIDPOD() {}
        explicit ExecutionGraphIDPOD(const std::vector<uint8_t>& bytes) : AbstractIDPOD(bytes) {}
    private:
        ExecutionGraphIDPOD(int64_t lowerPart, int64_t upperPart) : AbstractIDPOD(lowerPart, upperPart) {}
    };

} // namespace omnistream

#endif // OMNISTREAM_EXECUTIONGRAPHIDPOD_H