/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "CommitterOperator.h"


CommitterOperator::CommitterOperator(
    bool emitDownstream,
    bool isBatchMode,
    bool isCheckpointingEnabled)
    : emitDownstream(emitDownstream),
      isBatchMode(isBatchMode),
      isCheckpointingEnabled(isCheckpointingEnabled) {}


CommitterOperator::CommitterOperator(bool isBatch) : isDataStream(!isBatch)
{
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
