#include <gtest/gtest.h>
#include "core/task/StreamTask.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
#include <fstream>

using namespace std;
using namespace testing;
using json = nlohmann::json;

static omnistream::datastream::StreamTask *createTaskTest() {
    auto env = make_shared<omnistream::RuntimeEnvironmentV2>();
    // Create JSON for testing
    std::string ntdd = "{\"partition\":\n"
                       "                {\"partitionName\":\"forward\",\"channelNumber\":1},\n"
                       "            \"operators\":[\n"
                       "                {\"output\":{\"kind\":\"basic\",\"type\":\"Long\"},\n"
                       "                \"inputs\":[{\"kind\":\"basic\",\"type\":\"Long\"}],\n"
                       "                \"name\":\"Map\",\n"
                       "                \"description\":{\"inputTypes\":[{\"filed\":\"Long\",\"typeName\":\"org.apache.flink.api.common.typeutils.base.LongSerializer\"}],\"key_so\":\"\",\"udf_so\":\"/tmp/libMockMapFunction.so\",\n"
                       "                    \"index\":2,\n"
                       "                    \"outputTypes\":{\"filed\":\"Long\",\"typeName\":\"org.apache.flink.api.common.typeutils.base.LongSerializer\"},\n"
                       "                    \"originDescription\":\"Map\"},\n"
                       "                \"id\":\"org.apache.flink.streaming.api.operators.StreamMap\"}],\n"
                       "            \"type\":\"DataStream\"}";

    omnistream::StreamConfigPOD streamConfigPod;
    omnistream::OperatorPOD operatorPod;
    std::string  id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description = R"({"udf_so":"/tmp/libMockMapFunction.so","udf_obj":"{}"})";
    operatorPod.setDescription(description);
    operatorPod.setId(id);
    streamConfigPod.setOperatorDescription(operatorPod);
    omnistream::TaskInformationPOD taskInformationPOD;
    taskInformationPOD.setStreamConfigPOD(streamConfigPod);

    env->setTaskConfiguration(taskInformationPOD);
    OutputBufferStatus outputBufferStatus;
    outputBufferStatus.outputBuffer_ = reinterpret_cast<uintptr_t>(reinterpret_cast<uint8_t *>(malloc(1024)));
    outputBufferStatus.capacity_ = 1024;


    omnistream::datastream::StreamTask *streamTask = new omnistream::datastream::StreamTask(json::parse(ntdd), &outputBufferStatus,
                                                                                            env);
    return streamTask;
}


TEST(StreamTaskTest, ExtractTaskPartitionerConfigValid) {

    omnistream::datastream::StreamTask *streamTask = createTaskTest();
    json ntdd = R"(
    {
        "partition": {
            "partitionName": "forward",
            "channelNumber": 1
        }
    })"_json;

    omnistream::datastream::TaskPartitionerConfig config = streamTask->extractTaskPartitionerConfig(ntdd);
    EXPECT_EQ(config.getPartitionName(), "forward");
    EXPECT_EQ(config.getNumberOfChannel(), 1);
    delete streamTask;
}

TEST(StreamTaskTest, ExtractTaskPartitionerConfigInvalid) {
    omnistream::datastream::StreamTask *streamTask = createTaskTest();
    json ntdd = R"({})"_json;

    omnistream::datastream::TaskPartitionerConfig config = streamTask->extractTaskPartitionerConfig(ntdd);
    EXPECT_EQ(config.getPartitionName(), "forward");
    EXPECT_EQ(config.getNumberOfChannel(), 1);
    delete streamTask;
}

TEST(StreamTaskTest, CreatePartitionerForward) {
    omnistream::datastream::StreamTask *streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("forward", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable> *partitioner = streamTask->createPartitioner();
    EXPECT_NE(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}

TEST(StreamTaskTest, CreatePartitionerResacale) {
    omnistream::datastream::StreamTask *streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("rescale", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable> *partitioner = streamTask->createPartitioner();
    EXPECT_NE(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}

TEST(StreamTaskTest, CreatePartitionerRebalance) {
    omnistream::datastream::StreamTask *streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("rebalance", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable> *partitioner = streamTask->createPartitioner();
    EXPECT_NE(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}

TEST(StreamTaskTest, CreatePartitionerHash) {
    omnistream::datastream::StreamTask *streamTask = createTaskTest();
    streamTask->setTaskPartitionerConfig(omnistream::datastream::TaskPartitionerConfig("hash", 1, nullptr));
    omnistream::datastream::StreamPartitioner<IOReadableWritable> *partitioner = streamTask->createPartitioner();
    EXPECT_EQ(partitioner, nullptr);
    delete partitioner;
    delete streamTask;
}
