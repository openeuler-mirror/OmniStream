#include "table/runtime/operators/sink/SinkOperator.h"
#include "runtime/operators/sink/TimeStampInserterSinkOperator.h"
#include "typeutils/BinaryRowDataSerializer.h"
#include "writer/RecordWriter.h"
#include "io/RecordWriterOutput.h"
#include "test/table/runtime/operators/DummyStreamPartitioner.h"
#include <gtest/gtest.h>
#include <vector/vector_helper.h>
#include <test/util/test_util.h>

using namespace omniruntime::vec;
TEST(SinkOperatorTest, InitTest)
{
    std::string sinkDescription = R"({"outputfile":"/tmp/flink_output.txt"})";
    auto obj = nlohmann::json::parse(sinkDescription);
    auto op = SinkOperator(obj);
    op.open();

    BinaryRowData *rowData = BinaryRowData::createBinaryRowDataWithMem(1);
    StreamRecord *record = new StreamRecord(rowData);
    op.processElement(record);
};

TEST(SinkOperatorTest, DISABLED_BatchOutputTest) {
    std::string sinkDescription = R"({"outputfile":"/tmp/flink_output.txt"})";
    auto obj = nlohmann::json::parse(sinkDescription);
    auto op = SinkOperator(obj);
    int vectorSize = 5;

    omnistream::VectorBatch vb(vectorSize);
    Vector<int>* vec0 = new Vector<int>(vectorSize);
    for (int i = 0; i < vectorSize; i++) {
        vec0->SetValue(i, i);
    }
    vb.Append(vec0);

    auto *vec1 = new Vector<LargeStringContainer<std::string_view>>(vectorSize, 2048);

    std::string valuePrefix;
    valuePrefix = "hello_world__";

    for (int32_t i = 0; i < vectorSize; i++) {
        std::string value = valuePrefix + std::to_string(i);
        std::string_view input(value.data(), value.size());
        vec1->SetValue(i, input);
    }

    vb.Append(vec1);
    op.processBatch(new StreamRecord(&vb));
}

omnistream::VectorBatch* createVectorBatch()
{
    auto* vbatch = new omnistream::VectorBatch(5);
    std::vector<int64_t> bids = {9986, 9067, 7973, 9442, 9285};
    std::vector<int64_t> timeStamps = {5, 13, 18, 22, 27};
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(bids.size(), bids.data()));
    vbatch->Append(omniruntime::TestUtil::CreateVector<int64_t>(bids.size(), timeStamps.data()));
    for (int i = 0; i < bids.size(); i++) {
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    return vbatch;
}

TEST(TimeStampSinkOperatorTest, InitTest)
{
    std::string desc = R"DELIM({"rowtimeFieldIndex": 1})DELIM";
    auto obj = nlohmann::json::parse(desc);
    BinaryRowDataSerializer *binaryRowDataSerializer = new BinaryRowDataSerializer(2);
    const int32_t BUFFER_CAPACITY = 256;
    uint8_t *address = new uint8_t[BUFFER_CAPACITY];
    auto *partitioner = new DummyStreamPartitioner();
    auto * recordWriter = new omnistream::datastream::RecordWriter(address, BUFFER_CAPACITY, partitioner);
    auto *recordWriteOutput = new omnistream::datastream::RecordWriterOutput(binaryRowDataSerializer, recordWriter);
    auto op = TimeStampInserterSinkOperator(obj, recordWriteOutput, obj);
    op.open();
    BinaryRowData *rowData = BinaryRowData::createBinaryRowDataWithMem(1);
    StreamRecord *record = new StreamRecord(rowData);
    op.processElement(record);
    omnistream::VectorBatch* batch = createVectorBatch();
    StreamRecord *record2 = new StreamRecord(rowData);
    op.processBatch(record2);
}

