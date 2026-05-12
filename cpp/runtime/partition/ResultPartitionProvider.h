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

#ifndef OMNISTREAM_RESULTPARTITIONPROVIDER_H
#define OMNISTREAM_RESULTPARTITIONPROVIDER_H

#include <memory>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>

#include "ResultSubpartitionView.h"
#include "BufferAvailabilityListener.h"
// check
namespace omnistream {

    class ResultPartitionProvider {
    public:
        virtual ~ResultPartitionProvider() = default;

        virtual std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
            const ResultPartitionIDPOD& partitionId,
            int index,
            BufferAvailabilityListener* availabilityListener) = 0;
    };

} // namespace omnistream

#endif // OMNISTREAM_RESULTPARTITIONPROVIDER_H