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
#ifndef OMNISTREAM_SUBTASKCHECKPOINTCOORDINATOR_H
#define OMNISTREAM_SUBTASKCHECKPOINTCOORDINATOR_H

#include "runtime/checkpoint/CheckpointMetaData.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "runtime/checkpoint/CheckpointMetricsBuilder.h"
#include "OperatorChain.h"

namespace omnistream {
    class SubtaskCheckpointCoordinator {
    public:
        virtual ~SubtaskCheckpointCoordinator() = default;

        /**
         * Triggers a checkpoint asynchronously (typically for source tasks)
         *
         * @param checkpointMetaData Meta data about this checkpoint
         * @param checkpointOptions Options for performing this checkpoint
         * @return future with value of false if checkpoint was not carried out, true otherwise
         */
        virtual void InitInputsCheckpoint(long id, CheckpointOptions *checkpointOptions) = 0;

        virtual void checkpointState(
                CheckpointMetaData *metadata,
                CheckpointOptions *options,
                CheckpointMetricsBuilder *metrics,
                omnistream::OperatorChainV2 *operatorChain,
                bool isTaskFinished,
                omnistream::Supplier<bool> *isRunning
        ) {};
    };
}
#endif // OMNISTREAM_SUBTASKCHECKPOINTCOORDINATOR_H
