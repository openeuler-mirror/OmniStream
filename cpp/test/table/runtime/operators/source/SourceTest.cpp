/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */

#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>
#include "connector/kafka/source/reader/fetcher/KafkaSourceFetcherManager.h"
#include "connector/kafka/source/reader/SplitReader.h"
#include "connector/kafka/source/split/KafkaPartitionSplit.h"
#include "connector/kafka/source/reader/synchronization/FutureCompletingBlockingQueue.h"
#include "core/api/common/serialization/JsonRowDataDeserializationSchema.h"
#include "types/logical/LogicalType.h"
#include "runtime/operators/source/csv/CsvRow.h"
#include "runtime/operators/source/csv/CsvSchema.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "runtime/operators/source/csv/CsvInputFormat.h"
#include "streaming/api/operators/StreamSource.h"
#include "table/runtime/operators/source/InputSplit.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "streaming/runtime/io/RecordWriterOutput.h"
#include "test/table/runtime/operators/DummyStreamPartitioner.h"

namespace {
class BlockingKafkaSplitReader : public SplitReader<RdKafka::Message, KafkaPartitionSplit> {
public:
    RecordsWithSplitIds<RdKafka::Message>* fetch() override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        enteredFetch_ = true;
        fetchStarted_.notify_all();
        wakeSignal_.wait(lock, [this] { return closed_ || wokenUp_; });
        return nullptr;
    }

    void handleSplitsChanges(const std::vector<KafkaPartitionSplit*>&) override
    {
    }

    void wakeUp() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        wokenUp_ = true;
        wakeSignal_.notify_all();
    }

    void close() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        wakeSignal_.notify_all();
    }

    void waitUntilFetchStarts()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        fetchStarted_.wait(lock, [this] { return enteredFetch_; });
    }

private:
    std::mutex mutex_;
    std::condition_variable fetchStarted_;
    std::condition_variable wakeSignal_;
    bool enteredFetch_ = false;
    bool wokenUp_ = false;
    bool closed_ = false;
};

} // namespace

TEST(SourceTestTest, CsvRowKeepsQuotesInUnquotedJsonField)
{
    omnistream::csv::CsvSchema schema({omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_VARCHAR});
    omnistream::csv::CsvRow row("11,[\"only\"]", schema);

    ASSERT_EQ(row.getNodes().size(), 2);
    EXPECT_EQ(row.getNodes()[0]->getValue(), "11");
    EXPECT_EQ(row.getNodes()[1]->getValue(), "[\"only\"]");
}

TEST(SourceTestTest, CsvRowParsesQuotedJsonField)
{
    omnistream::csv::CsvSchema schema({omniruntime::type::OMNI_LONG, omniruntime::type::OMNI_VARCHAR});
    omnistream::csv::CsvRow row("11,\"[\"\"only\"\"]\"", schema);

    ASSERT_EQ(row.getNodes().size(), 2);
    EXPECT_EQ(row.getNodes()[0]->getValue(), "11");
    EXPECT_EQ(row.getNodes()[1]->getValue(), "[\"only\"]");
}

TEST(SourceTestTest, JsonRowDataDeserializationSchemaHandlesNullStringField)
{
    nlohmann::json description = {
        {"outputNames", {"id", "json_str"}},
        {"outputTypes", {"BIGINT", "VARCHAR(2147483647)"}}
    };
    JsonRowDataDeserializationSchema schema(description);

    std::string record = R"({"id":1,"json_str":null})";
    std::vector<const uint8_t*> messages{
        reinterpret_cast<const uint8_t*>(record.data())
    };
    std::vector<size_t> lengths{record.size()};

    auto* batch = reinterpret_cast<omnistream::VectorBatch*>(schema.deserialize(messages, lengths));
    ASSERT_NE(batch, nullptr);

    auto* idVector = reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(0));
    auto* jsonVector = reinterpret_cast<omniruntime::vec::Vector<
        omniruntime::vec::LargeStringContainer<std::string_view>>*>(batch->Get(1));

    EXPECT_FALSE(idVector->IsNull(0));
    EXPECT_EQ(idVector->GetValue(0), 1);
    EXPECT_TRUE(jsonVector->IsNull(0));

    delete batch;
}

TEST(SourceTestTest, SplitFetcherManagerCloseJoinsExecutorThreads)
{
    auto* queue = new FutureCompletingBlockingQueue<RdKafka::Message>(0);
    auto readerHolder = std::make_shared<BlockingKafkaSplitReader*>(nullptr);
    std::function<SplitReader<RdKafka::Message, KafkaPartitionSplit>*()> supplier =
        [readerHolder]() {
            auto* reader = new BlockingKafkaSplitReader();
            *readerHolder = reader;
            return reader;
        };
    auto* manager = new KafkaSourceFetcherManager(queue, supplier);

    auto topicPartition = std::shared_ptr<RdKafka::TopicPartition>(
        RdKafka::TopicPartition::create("json_topic", 0));
    std::vector<KafkaPartitionSplit*> splits{
        new KafkaPartitionSplit(topicPartition, 0)
    };

    manager->addSplits(splits);
    while (*readerHolder == nullptr) {
        std::this_thread::yield();
    }
    (*readerHolder)->waitUntilFetchStarts();

    manager->close(1000);
    EXPECT_EQ(manager->getNumAliveFetchers(), 0);

    delete manager;
    delete queue;
    delete splits[0];
}

TEST(SourceTestTest, DISABLED_initoper)
{
    std::string description = R"DELIM({ "format" : "csv",
                        "delimiter" : null,
                        "filePath" : "input/genbid.csv",
                        "fields" : [{"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "auction"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "bidder"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "BIGINT", "timestampKind" : 0, "fieldName" : "price"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "channel"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "url"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "TIMESTAMP(3)", "timestampKind" : 0, "fieldName" : "dateTime"  },
                                    {"kind" : "logical", "isNull" : true, "precision" : 3,
                                        "type" : "STRING", "timestampKind" : 0, "fieldName" : "extra"  }
                                    ],
                        "selectFields" : [ 0, 1, 2, 3, 4 ],
                        "csvSelectFieldToProjectFieldMapping" : [ 0, 1, 2, 3, 4 ],
                        "csvSelectFieldToCsvFieldMapping" : [ 3, 5, 2, 1, 0 ]})DELIM";
    nlohmann::json opDescriptionJSON = nlohmann::json::parse(description);
    std::vector<DataTypeId> fields;
    for (auto &field: opDescriptionJSON["fields"]) {
        fields.push_back(LogicalType::flinkTypeToOmniTypeId(field["type"]));
    }
    omnistream::csv::CsvSchema schema(fields);
    std::vector<int> csvSelectFieldToProjectFieldMapping = opDescriptionJSON["csvSelectFieldToProjectFieldMapping"];
    std::vector<int> csvSelectFieldToCsvFieldMapping = opDescriptionJSON["csvSelectFieldToCsvFieldMapping"];
    std::vector<int> oneMap;
    oneMap.resize(csvSelectFieldToProjectFieldMapping.size());
    for (int i = 0; i < csvSelectFieldToProjectFieldMapping.size(); i++) {
        oneMap[csvSelectFieldToProjectFieldMapping[i]] = csvSelectFieldToCsvFieldMapping[i];
    }
    auto csvInputFormat = new omnistream::csv::CsvInputFormat<omnistream::VectorBatch>(schema, 1000,
                                                                            oneMap);
    omnistream::InputSplit *inputSplit = new omnistream::InputSplit(opDescriptionJSON["filePath"], 0,
                                                                    100000);
    auto func = new omnistream::InputFormatSourceFunction<omnistream::VectorBatch>(csvInputFormat, inputSplit);
    BinaryRowDataSerializer *binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t *address = new uint8_t[BUFFER_CAPACITY];
    auto *partitioner = new DummyStreamPartitioner();
    auto *recordWriter = new omnistream::datastream::RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto *recordWriteOutput = new omnistream::datastream::RecordWriterOutput(binaryRowDataSerializer, recordWriter);
    auto *source = new omnistream::StreamSource(func, recordWriteOutput);
    source->setup();
    source->run();
}