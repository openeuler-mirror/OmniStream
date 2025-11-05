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

#ifndef OMNIFLINK_COMMITTEROPERATOR_H
#define OMNIFLINK_COMMITTEROPERATOR_H

#include <memory>
#include <vector>
#include <optional>
#include <functional>
#include <limits>
#include "streaming/api/operators/sink/committables/CommittableMessage.h"
#include "streaming/api/operators/sink/committables/CheckpointCommittableManager.h"
#include "streaming/api/operators/sink/committables/CommittableCollector.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/OneInputStreamOperator.h"

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
