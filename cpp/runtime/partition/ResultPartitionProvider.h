/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/3/25.
//


// ResultPartitionProvider.h
#ifndef OMNISTREAM_RESULTPARTITIONPROVIDER_H
#define OMNISTREAM_RESULTPARTITIONPROVIDER_H

#include <memory>
#include <executiongraph/descriptor/ResultPartitionIDPOD.h>

#include "ResultSubpartitionView.h"
#include "BufferAvailabilityListener.h"

namespace omnistream {

    class ResultPartitionProvider {
    public:
        virtual ~ResultPartitionProvider() = default;

        virtual std::shared_ptr<ResultSubpartitionView> createSubpartitionView(
            const ResultPartitionIDPOD& partitionId,
            int index,
            std::shared_ptr<BufferAvailabilityListener> availabilityListener) = 0;
    };

} // namespace omnistream

#endif // OMNISTREAM_RESULTPARTITIONPROVIDER_H