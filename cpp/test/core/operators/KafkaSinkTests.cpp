#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include "connector-kafka/sink/KafkaSink.h"
#include "core/operators/sink/SinkWriterOperator.h"
#include <ctime>

using json = nlohmann::json;

const std::unordered_map<std::string, DeliveryGuarantee> deliveryGuaranteeMap = {
        {"EXACTLY_ONCE", DeliveryGuarantee::EXACTLY_ONCE},
        {"NONE", DeliveryGuarantee::NONE},
        {"AT_LEAST_ONCE", DeliveryGuarantee::AT_LEAST_ONCE}
};

//std::string generateRandomString(size_t length) {
//    const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
//    std::string result;
//    result.reserve(length);
//
//    for (size_t i = 0; i < length; ++i) {
//        result += charset[rand() % (sizeof(charset) - 1)]; // 随机选择字符
//    }
//
//    return result;
//}

TEST(KafkaSinkTest, DISABLED_StringDataSink) {
    std::string jsonString = R"({
        "deliveryGuarantee": "AT_LEAST_ONCE",
        "transactionalIdPrefix": "kafka-sink",
        "properties": {
            "bootstrap.servers": "127.0.0.1:9092",
            "transaction.timeout.ms": "600000"
        },
        "topic": "kafka-sink",
        "inputTypes": [],
        "inputFields": []
    })";
    json tableOptions = json::parse(jsonString);

    if (!tableOptions.contains("deliveryGuarantee")) {
        throw std::invalid_argument("JSON object does not contain 'deliveryGuarantee' field.");
    }
    std::string guaranteeStr = tableOptions["deliveryGuarantee"];
    auto it = deliveryGuaranteeMap.find(guaranteeStr);
    if (it == deliveryGuaranteeMap.end()) {
        throw std::invalid_argument("Invalid value for 'deliveryGuarantee': " + guaranteeStr);
    }
    auto deliveryGuarantee = it->second;

    std::string transactionalIdPrefix = tableOptions["transactionalIdPrefix"];
    std::string topic = tableOptions["topic"];
    auto kafkaProducerConfig = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    for (auto& item : tableOptions["properties"].items()) {
        std::string key = item.key();
        if (key.find("bootstrap.servers") || key.find("transaction.timeout.ms")) {
            std::string value = item.value();
            std::string errorString;
            RdKafka::Conf::ConfResult result = kafkaProducerConfig->set(key, value, errorString);
            if (result != RdKafka::Conf::CONF_OK) {
                std::cerr << "Failed to set Kafka configuration: " << key << " = " << value << std::endl;
            }
        }
    }
    auto kafkaSink = new KafkaSink(deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix, topic, tableOptions,
                                   100);
    SinkWriterOperator *sinkWriterOperator;
    std::vector<std::any> fieldByPosition;
    std::map<std::string, std::any> fieldByName;
    std::map<std::string, int> positionByName;

    fieldByPosition.push_back(-7908116295878520943);
    fieldByPosition.push_back(8.989201265750876E307);
    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    fieldByPosition.push_back(millis);
    auto row = new Row(RowKind::INSERT, fieldByPosition, fieldByName, positionByName);
    auto streamRecord = new StreamRecord(row);

    try {
        sinkWriterOperator = new SinkWriterOperator(kafkaSink, tableOptions);
//        srand(static_cast<unsigned int>(time(0)));
//        size_t length = 10;
//        std::string randomString = generateRandomString(length);
        for (int i = 0; i < 100000; ++i) {
            sinkWriterOperator->processElement(streamRecord);
        }
    } catch (std::runtime_error& e) {
        std::cout << "Cannot connect to Kafka, please check kafka server." << std::endl;
    } catch (std::exception& e) {
        std::cout << " " << e.what() << std::endl;
    }
//    delete row;
//    delete streamRecord;
//    delete kafkaSink;
//    delete sinkWriterOperator;
    EXPECT_EQ(0, 0);
}