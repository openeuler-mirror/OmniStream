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

#include <gtest/gtest.h>

#include <iostream>
#include <map>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <unordered_map>

#include <librdkafka/rdkafkacpp.h>

// Pre-include KafkaSourceReader.h's direct dependencies so that their
// include guards are set before the `#define private public` below.
// This ensures only KafkaSourceReader.h is affected by the macro.
#include "connector/kafka/source/metrics/KafkaSourceReaderMetrics.h"
#include "connector/kafka/source/reader/SingleThreadMultiplexSourceReaderBase.h"

#define private public
#include "connector/kafka/source/reader/KafkaSourceReader.h"
#undef private

#include "connector/kafka/source/reader/RecordEmitter.h"
#include "connector/kafka/source/reader/fetcher/KafkaSourceFetcherManager.h"
#include "connector/kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/connector/source/SourceReaderContext.h"

namespace {
// Minimal RecordEmitter mock that does nothing on emit.
class MockRecordEmitter : public RecordEmitter<RdKafka::Message, KafkaPartitionSplitState> {
public:
    void emitRecord(RdKafka::Message*, SourceOutput*, KafkaPartitionSplitState*) override
    {
    }
    void emitBatchRecord(const std::vector<RdKafka::Message*>&, SourceOutput*, KafkaPartitionSplitState*) override
    {
    }
};
} // namespace

class KafkaSourceReaderConfigTest : public ::testing::Test {
protected:
    // Create a KafkaSourceReader with the given props.
    // The reader takes ownership of all heap-allocated dependencies.
    std::unique_ptr<KafkaSourceReader> createReader(
        const std::unordered_map<std::string, std::string>& props, bool isBatch = false)
    {
        auto* elementsQueue = new FutureCompletingBlockingQueue<RdKafka::Message>(0);
        std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()> supplier =
            []() -> SplitReader<RdKafka::Message, KafkaPartitionSplit>* { return nullptr; };
        auto* fetcherManager = new KafkaSourceFetcherManager(elementsQueue, supplier);
        auto* recordEmitter = new MockRecordEmitter();
        auto* context = new SourceReaderContext(0);
        return std::unique_ptr<KafkaSourceReader>(
            new KafkaSourceReader(elementsQueue, fetcherManager, recordEmitter, props, context, isBatch));
    }
};

// "false" -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsFalse)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "false";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// "true" -> enabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsEnabledWhenConfigIsTrue)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "true";
    auto reader = createReader(props);
    EXPECT_TRUE(reader->commitOffsetsOnCheckpoint_);
}

// Missing property -> disabled (default)
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledByDefault)
{
    std::unordered_map<std::string, std::string> props;
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Non-"true" value -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsNotTrue)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "yes";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Empty string value -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsEmpty)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Case sensitivity: "True" is not "true" -> disabled
TEST_F(KafkaSourceReaderConfigTest, CommitOffsetsDisabledWhenConfigIsCaseSensitive)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "True";
    auto reader = createReader(props);
    EXPECT_FALSE(reader->commitOffsetsOnCheckpoint_);
}

// Behavioral: when disabled, snapshotState returns early and
// notifyCheckpointComplete does not touch offsetsToCommit_.
TEST_F(KafkaSourceReaderConfigTest, SnapshotAndNotifyWhenDisabled)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "false";
    auto reader = createReader(props);
    auto splits = reader->snapshotState(1);
    EXPECT_TRUE(splits.empty());
    // No crash: notifyCheckpointComplete returns before accessing offsetsToCommit_.
    reader->notifyCheckpointComplete(1);
    // offsetsToCommit_ should remain empty since snapshotState returned early.
    EXPECT_TRUE(reader->offsetsToCommit_.empty());
}

// Behavioral: when enabled, snapshotState populates offsetsToCommit_ and
// notifyCheckpointComplete drains it.
TEST_F(KafkaSourceReaderConfigTest, SnapshotAndNotifyWhenEnabled)
{
    std::unordered_map<std::string, std::string> props;
    props["commit.offsets.on.checkpoint"] = "true";
    auto reader = createReader(props);
    auto splits = reader->snapshotState(1);
    EXPECT_TRUE(splits.empty());
    // snapshotState should have recorded an (empty) offsets entry for checkpoint 1.
    EXPECT_FALSE(reader->offsetsToCommit_.empty());
    EXPECT_EQ(reader->offsetsToCommit_.count(1), 1u);
    // notifyCheckpointComplete will call commitOffsets with an empty map,
    // which returns early (no crash).
    reader->notifyCheckpointComplete(1);
}
