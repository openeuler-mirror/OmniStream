#include <gtest/gtest.h>
#include <iostream>
#include "core/operators/StreamMap.h"
#include "test_utils/Mocks.h"

using namespace std;

// 测试 StreamMap 的构造函数
TEST(StreamMapTest, Constructor_ValidPath) {
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

// 检查 output 是否正确设置
    EXPECT_EQ(&output, streamMap.getOutput());
}

TEST(StreamMapTest, Constructor_InvalidPath) {
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "invalid_path.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output);
    EXPECT_THROW(streamMap.loadUdf(config), std::out_of_range);
}

// 测试 processElement 方法
/*TEST(StreamMapTest, ProcessElement_Valid) {
    MockOutput *output = new MockOutput();
    StreamMap<MapFunction, int> *streamMap = new StreamMap<MapFunction, int>(output, "/tmp/libMockMapFunction.so");

    MockObject *input = new MockObject(10);
    StreamRecord *record = new StreamRecord(input);
    streamMap->processElement(record);

// 检查输出是否正确
    vector<StreamRecord*> collectedRecords = output->getCollectedRecords();
    EXPECT_EQ(collectedRecords.size(), 1);
    EXPECT_EQ(dynamic_cast<MockObject *>(collectedRecords[0]->getValue())->getValue(), 11);

    delete collectedRecords[0]->getValue();
    delete collectedRecords[0];
    delete input;
    delete streamMap;
    delete output;
}*/

TEST(StreamMapTest, ProcessElement_Invalid) {
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);


// 传递 nullptr 作为 StreamRecord 的 value
    StreamRecord record(nullptr);
    streamMap.processElement(&record);
}

// 测试 getName 方法
TEST(StreamMapTest, GetName) {
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

    EXPECT_STREQ(streamMap.getName(), "StreamMap");
}

// 测试 open 方法
TEST(StreamMapTest, Open_NotImplemented) {
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

    streamMap.open();

// 不需要额外的检查，因为该方法未实现
}

// 测试 close 方法
TEST(StreamMapTest, Close_NotImplemented) {
    MockOutput output;
    nlohmann::json config;
    config["udf_so"] = "/tmp/libMockMapFunction.so";
    config["udf_obj"] = "{}";
    omnistream::datastream::StreamMap<Object, Object*> streamMap(&output, config);

    streamMap.close();
}
