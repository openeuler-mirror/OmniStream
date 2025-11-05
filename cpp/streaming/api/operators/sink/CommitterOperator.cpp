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

#include "CommitterOperator.h"


CommitterOperator::CommitterOperator(
    bool emitDownstream,
    bool isBatchMode,
    bool isCheckpointingEnabled)
    : emitDownstream(emitDownstream),
      isBatchMode(isBatchMode),
      isCheckpointingEnabled(isCheckpointingEnabled) {
    isDataStream = false;
    endOfInput = false;
}


CommitterOperator::CommitterOperator(bool isBatch) : isDataStream(!isBatch)
{
    endOfInput = false;
    isBatchMode = false;
    isCheckpointingEnabled = false;
    emitDownstream = false;
}


void CommitterOperator::setup() {
}

void CommitterOperator::open() {
}

void CommitterOperator::initializeState() {}


void CommitterOperator::SnapshotState() {
}


void CommitterOperator::EndInput() {
}


void CommitterOperator::NotifyCheckpointComplete(long checkpointId) {
}


void CommitterOperator::CommitAndEmitCheckpoints() {
}


void CommitterOperator::CommitAndEmit(std::shared_ptr<CheckpointCommittableManager<int>> committableManager,
                                      bool fullyReceived) {}


void CommitterOperator::RetryWithDelay() {}


void CommitterOperator::processElement(StreamRecord& element) {
}


void CommitterOperator::close() {
}
