/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <nlohmann/json.hpp>

#include "executiongraph/OckDBConfigPOD.h"
#include "executiongraph/TaskInformationPOD.h"

using json = nlohmann::json;

TEST(OckDBConfigPODTest, DefaultValuesMatchNativeContract)
{
    omnistream::OckDBConfigPOD pod;

    EXPECT_EQ(4, pod.getCheckpointTransferThreadNum());
    EXPECT_TRUE(pod.getBackupDirectory().empty());
    EXPECT_TRUE(pod.getLocalDirectories().empty());
    EXPECT_EQ("HEAP", pod.getPriorityQueueType());
    EXPECT_EQ("/usr/local/flink/log/kv.log", pod.getJniLogDirectory());
    EXPECT_EQ(20 * 1024 * 1024, pod.getJniLogSizeBytes());
    EXPECT_FLOAT_EQ(0.8F, pod.getJniSliceWatermarkRatio());
    EXPECT_FLOAT_EQ(0.2F, pod.getFileMemoryFraction());
    EXPECT_EQ("lz4", pod.getLsmCompressionPolicy());
    EXPECT_TRUE(pod.getCacheFilterAndIndexSwitch());
    EXPECT_TRUE(pod.getBloomFilterSwitch());
    EXPECT_FALSE(pod.getKvSeparateSwitch());
    EXPECT_EQ(200, pod.getKvSeparateThreshold());
}

TEST(OckDBConfigPODTest, JsonRoundTripPreservesConfiguration)
{
    omnistream::OckDBConfigPOD pod;
    pod.setCheckpointTransferThreadNum(8);
    pod.setBackupDirectory("/tmp/backup");
    pod.setLocalDirectories("/tmp/db1,/tmp/db2");
    pod.setPriorityQueueType("OCKDB");
    pod.setJniLogDirectory("/var/log/ock.log");
    pod.setJniLogSizeBytes(50 * 1024 * 1024);
    pod.setJniSliceWatermarkRatio(0.9F);
    pod.setFileMemoryFraction(0.3F);
    pod.setLsmCompressionPolicy("zstd");
    pod.setKvSeparateSwitch(true);
    pod.setKvSeparateThreshold(512);

    omnistream::OckDBConfigPOD restored = json(pod).get<omnistream::OckDBConfigPOD>();

    EXPECT_EQ(8, restored.getCheckpointTransferThreadNum());
    EXPECT_EQ("/tmp/backup", restored.getBackupDirectory());
    EXPECT_EQ("/tmp/db1,/tmp/db2", restored.getLocalDirectories());
    EXPECT_EQ("OCKDB", restored.getPriorityQueueType());
    EXPECT_EQ("/var/log/ock.log", restored.getJniLogDirectory());
    EXPECT_EQ(50 * 1024 * 1024, restored.getJniLogSizeBytes());
    EXPECT_FLOAT_EQ(0.9F, restored.getJniSliceWatermarkRatio());
    EXPECT_FLOAT_EQ(0.3F, restored.getFileMemoryFraction());
    EXPECT_EQ("zstd", restored.getLsmCompressionPolicy());
    EXPECT_TRUE(restored.getKvSeparateSwitch());
    EXPECT_EQ(512, restored.getKvSeparateThreshold());
}

TEST(OckDBConfigPODTest, MissingJsonFieldsUseDefaults)
{
    json partial = {
        {"checkpointTransferThreadNum", 16},
        {"priorityQueueType", "OCKDB"},
    };

    auto pod = partial.get<omnistream::OckDBConfigPOD>();

    EXPECT_EQ(16, pod.getCheckpointTransferThreadNum());
    EXPECT_EQ("OCKDB", pod.getPriorityQueueType());
    EXPECT_TRUE(pod.getBackupDirectory().empty());
    EXPECT_EQ("lz4", pod.getLsmCompressionPolicy());
    EXPECT_TRUE(pod.getBloomFilterSwitch());
    EXPECT_EQ(200, pod.getKvSeparateThreshold());
}

TEST(TaskInformationPODTest, StateBackendResourceContractRoundTrip)
{
    json input = {
        {"stateBackendConfigVersion", 1},
        {"stateBackendResourceId", static_cast<uint64_t>(UINT32_MAX)},
    };

    auto taskInfo = input.get<omnistream::TaskInformationPOD>();
    EXPECT_EQ(1U, taskInfo.getStateBackendConfigVersion());
    EXPECT_EQ(static_cast<uint64_t>(UINT32_MAX), taskInfo.getStateBackendResourceId());

    json output = taskInfo;
    EXPECT_EQ(1U, output.at("stateBackendConfigVersion").get<uint32_t>());
    EXPECT_EQ(static_cast<uint64_t>(UINT32_MAX), output.at("stateBackendResourceId").get<uint64_t>());
}

#ifdef WITH_OMNISTATESTORE
#include "state/bss/BssExceptionUtils.h"
#include "state/KeyGroupRange.h"
#include "state/KeyedStateHandle.h"
#include "state/LocalRecoveryConfig.h"
#include "state/ockdb/EmbeddedOckStateBackend.h"
#include "state/ockdb/OckDBCheckpointConfig.h"
#include "state/ockdb/OckDBKeyedStateBackendBuilder.h"
#include "typeutils/LongSerializer.h"

TEST(BssExceptionUtilsTest, CheckTableReturnsVoidAndAcceptsValidTable)
{
    auto table = std::make_shared<int>(1);
    static_assert(std::is_void_v<decltype(bss_adapter::CheckTable(table, "state"))>);

    EXPECT_NO_THROW(bss_adapter::CheckTable(table, "state"));
}

TEST(BssExceptionUtilsTest, CheckTableLogsBeforeThrowing)
{
    std::shared_ptr<int> table;

    testing::internal::CaptureStdout();
    EXPECT_THROW(bss_adapter::CheckTable(table, "missing-state"), std::runtime_error);
    const std::string output = testing::internal::GetCapturedStdout();

    EXPECT_NE(std::string::npos, output.find("[ERROR]"));
    EXPECT_NE(std::string::npos, output.find("missing-state"));
}

TEST(BssExceptionUtilsTest, CheckResultLogsBeforeThrowing)
{
    const auto error = static_cast<ock::bss::BResult>(-1);

    testing::internal::CaptureStdout();
    EXPECT_THROW(bss_adapter::CheckResult(error, "test-operation"), std::runtime_error);
    const std::string output = testing::internal::GetCapturedStdout();

    EXPECT_NE(std::string::npos, output.find("[ERROR]"));
    EXPECT_NE(std::string::npos, output.find("test-operation"));
}

TEST(EmbeddedOckStateBackendTest, InvalidResourceContractLogsBeforeThrowing)
{
    omnistream::TaskInformationPOD taskInfo;
    taskInfo.setStateBackendConfigVersion(2);

    testing::internal::CaptureStdout();
    EXPECT_THROW(EmbeddedOckStateBackend backend(taskInfo), std::invalid_argument);
    const std::string output = testing::internal::GetCapturedStdout();

    EXPECT_NE(std::string::npos, output.find("[ERROR]"));
    EXPECT_NE(std::string::npos, output.find("Unsupported state backend config version"));
}

TEST(OckDBCheckpointConfigTest, SetterGetterRoundTrip)
{
    OckDBCheckpointConfig config;
    config.setEnableIncrementalCheckpointing(true);
    config.setNumberOfTransferringThreads(8);
    config.setTaskSlotFlag(42);
    config.setTaskSlotMemoryLimit(64 * 1024 * 1024);
    config.setSlotManagedMemoryFraction(0.5);
    config.setJobID("job-123");
    config.setBackupDirectory("/tmp/backup");

    EXPECT_TRUE(config.isEnableIncrementalCheckpointing());
    EXPECT_EQ(8, config.getNumberOfTransferringThreads());
    EXPECT_EQ(42U, config.getTaskSlotFlag());
    EXPECT_EQ(64 * 1024 * 1024, config.getTaskSlotMemoryLimit());
    EXPECT_DOUBLE_EQ(0.5, config.getSlotManagedMemoryFraction());
    EXPECT_EQ("job-123", config.getJobID());
    EXPECT_EQ("/tmp/backup", config.getBackupDirectory());
}

TEST(OckDBKeyedStateBackendBuilderTest, CheckpointConfigIsSingleSourceOfTruth)
{
    using Builder = OckDBKeyedStateBackendBuilder<int64_t>;

    auto* keyGroupRange = new KeyGroupRange(0, 1);
    auto localRecoveryConfig = std::make_shared<LocalRecoveryConfig>(nullptr);
    std::vector<std::shared_ptr<KeyedStateHandle>> stateHandles;
    Builder builder(
        128,
        keyGroupRange,
        LongSerializer::INSTANCE,
        "/tmp/omnistream-bss-ut",
        localRecoveryConfig,
        stateHandles,
        OckDBCheckpointConfig::PriorityQueueStateType::HEAP);

    OckDBCheckpointConfig initial;
    initial.setEnableIncrementalCheckpointing(true);
    initial.setNumberOfTransferringThreads(8);
    initial.setTaskSlotFlag(42);
    initial.setTaskSlotMemoryLimit(128 * 1024 * 1024);
    initial.setJobID("initial-job");
    builder.setCheckpointConfig(initial);

    builder.setNumberOfTransferringThreads(6)
        .setTaskSlotFlag(99)
        .setTaskSlotMemoryLimit(256 * 1024 * 1024)
        .setJobID("updated-job")
        .setAsyncSnapshots(false);

    const auto& actual = builder.getCheckpointConfig();
    EXPECT_TRUE(actual.isEnableIncrementalCheckpointing());
    EXPECT_EQ(6, actual.getNumberOfTransferringThreads());
    EXPECT_EQ(99U, actual.getTaskSlotFlag());
    EXPECT_EQ(256 * 1024 * 1024, actual.getTaskSlotMemoryLimit());
    EXPECT_EQ("updated-job", actual.getJobID());
    EXPECT_FALSE(actual.isAsyncSnapshots());

    delete keyGroupRange;
}
#endif
