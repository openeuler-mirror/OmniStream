/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#ifndef OMNIFLINK_COMMITTEROPERATOR_H
#define OMNIFLINK_COMMITTEROPERATOR_H

#include <memory>
#include <vector>
#include <optional>
#include <functional>
#include <limits>
#include "core/committables/CommittableMessage.h"
#include "core/committables/CheckpointCommittableManager.h"
#include "core/committables/CommittableCollector.h"
#include "core/streamrecord/StreamRecord.h"
#include "core/operators/OneInputStreamOperator.h"

class CommitterOperator : public OneInputStreamOperator {
public:
    static constexpr long RETRY_DELAY = 1000;

    CommitterOperator(
            bool emitDownstream,
            bool isBatchMode,
            bool isCheckpointingEnabled);

    explicit CommitterOperator(bool isBatch);

    void setup();

    void open() override;

    void initializeState();

    void initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer) override {}

    void SnapshotState();

    void EndInput();

    void NotifyCheckpointComplete(long checkpointId);

    void processElement(StreamRecord &element);

    void processElement(StreamRecord *record) override {}

    void processBatch(StreamRecord *record) override {}

    void ProcessWatermark(Watermark *watermark) override {}
    void processWatermarkStatus(WatermarkStatus *watermarkStatus) override {}

    void close();

    bool canBeStreamOperator() override
    {
        return isDataStream;
    }

private:
    void CommitAndEmitCheckpoints();
    void CommitAndEmit(std::shared_ptr<CheckpointCommittableManager<int>> committableManager, bool fullyReceived);
    void RetryWithDelay();

    bool isDataStream;
    std::shared_ptr<Committer<int>> committer;
    bool emitDownstream;
    bool isBatchMode;
    bool isCheckpointingEnabled;
    std::shared_ptr<CommittableCollector<int>> committableCollector;
    long lastCompletedCheckpointId = -1;
    bool endOfInput;
};
#endif
