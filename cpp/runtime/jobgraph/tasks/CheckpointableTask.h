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
#ifndef OMNISTREAM_CHECKPOINTABLETASK_H
#define OMNISTREAM_CHECKPOINTABLETASK_H
#pragma once

#include <memory>
#include <future>
#include "io/checkpointing/CheckpointException.h"
#include "core/utils/threads/CompletableFutureV2.h"
#include "runtime/checkpoint/CheckpointMetricsBuilder.h"
#include "runtime/checkpoint/CheckpointMetaData.h"
#include "runtime/checkpoint/CheckpointOptions.h"

namespace omnistream {

    class CheckpointableTask {
    public:
        virtual ~CheckpointableTask() = default;

        /**
         * Triggered when checkpoint is initiated by receiving barriers
         *
         * @param checkpointMetaData Meta data about this checkpoint
         * @param checkpointOptions Options for performing this checkpoint
         * @param checkpointMetrics Metrics about this checkpoint
         * @throws IOException if checkpoint fails
         */
        virtual void TriggerCheckpointOnBarrier(CheckpointMetaData* checkpointMetaData,
            CheckpointOptions* checkpointOptions, CheckpointMetricsBuilder* checkpointMetrics) = 0;

        /**
         * Aborts checkpoint due to cancellation barrier
         *
         * @param checkpointId ID of checkpoint to abort
         * @param cause Reason for aborting
         * @throws IOException if abort fails
         */
        virtual void abortCheckpointOnBarrier(
                long checkpointId,
                CheckpointException cause) = 0;
    };
}
#endif // OMNISTREAM_CHECKPOINTABLETASK_H
