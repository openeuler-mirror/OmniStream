#include <gtest/gtest.h>
#include "core/operators/StreamOperatorFactory.h"
#include "core/graph/OperatorConfig.h"
#include "core/task/WatermarkGaugeExposingOutput.h"
#include "core/operators/OneInputStreamOperator.h"
#include "test_utils/Mocks.h"
#include "nlohmann/json.hpp"
#include "runtime/executiongraph/StreamConfigPOD.h"
#include <unordered_map>


class MockStreamOperatorFactory : public omnistream::StreamOperatorFactory {
public:
    static StreamOperator* createOperatorAndCollector(omnistream::OperatorConfig &opConfig, WatermarkGaugeExposingOutput *chainOutput) {
        return StreamOperatorFactory::createOperatorAndCollector(opConfig, chainOutput);
    }

    static StreamOperator* createOperatorAndCollector(const std::string& id, const std::string& description, WatermarkGaugeExposingOutput *chainOutput) {
        omnistream::OperatorPOD operatorPod;
        operatorPod.setDescription(description);
        operatorPod.setId(id);
        return StreamOperatorFactory::createOperatorAndCollector(operatorPod, chainOutput, nullptr);
    }
};

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_Map) {
    std::string id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description = "{\"udf_so\":\"/tmp/libMockMapFunction.so\",\"udf_obj\":\"{}\"}";

    MockOutput output;
    StreamOperator* operatorAndCollector = MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output);
    EXPECT_NE(operatorAndCollector, nullptr);
    delete operatorAndCollector;
}

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_Map_InvalidSoPath) {
    std::string id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description = "{\"udf_so\":\"invalidMap.so\",\"udf_obj\":\"{}\"}";
    MockOutput output;
    EXPECT_THROW(MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output), std::out_of_range);
}

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_Reduce) {
    std::string id = "org.apache.flink.streaming.api.operators.StreamMap";
    std::string description = "{\"udf_so\":\"/tmp/libMockReduceFunction.so\",\"key_so\":\"libMockKeyedBy.so\",\"udf_obj\":\"{}\"}";
    MockOutput output;
    StreamOperator* operatorAndCollector = MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output);
    EXPECT_NE(operatorAndCollector, nullptr);
    delete operatorAndCollector;
}

TEST(StreamOperatorFactoryTest, CreateOperatorAndCollector_UnknownOperator) {
    std::string id = "UnknownOperator";
    std::string description = "{}";

    MockOutput output;
    EXPECT_EQ(MockStreamOperatorFactory::createOperatorAndCollector(id, description, &output), nullptr);
}
