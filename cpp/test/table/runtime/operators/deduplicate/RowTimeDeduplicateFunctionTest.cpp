#include "table/runtime/operators/deduplicate/RowTimeDeduplicateFunction.h"
#include <vector>
#include <nlohmann/json.hpp>
#include "core/streamrecord/StreamRecord.h"
#include "functions/Collector.h"
#include "core/operators/StreamOperatorFactory.h"
#include "core/typeutils/LongSerializer.h"
#include <gtest/gtest.h>
#include "table/data/binary/BinaryRowData.h"
#include "runtime/taskmanager/RuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "table/typeutils/RowDataSerializer.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include "../../../../core/operators/OutputTest.h"
#include "table/data/util/VectorBatchUtil.h"
#include <string.h>
#include "table/RowKind.h"

using namespace omnistream;

std::string uniqueName = "org.apache.flink.streaming.api.operators.KeyedProcessOperator";

std::string description = R"DELIM({"input_channels":[0],
                        "operators":[{"description":{
        "miniBatchSize": -1,
        "inputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR"
        ],
        "keepLastRow": true,
        "isCompactChanges": false,
        "minRetentionTime": 0,
        "rowtimeIndex": 2,
        "isRowtime": true,
        "generateUpdateBefore": true,
        "outputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR(2147483647)"
        ],
        "generateInsert": true,
        "originDescription": null,
        "grouping": [
          0, 1
        ]
      }}],
    "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

std::string ConfigStr_deduplicate =
    R"delimiter({
      "name": "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",
      "description": {
        "miniBatchSize": -1,
        "inputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR(2147483647)"
        ],
        "keepLastRow": true,
        "isCompactChanges": false,
        "minRetentionTime": 0,
        "rowtimeIndex": 2,
        "isRowtime": true,
        "generateUpdateBefore": true,
        "outputTypes": [
          "BIGINT",
          "BIGINT",
          "BIGINT",
          "VARCHAR(2147483647)"
        ],
        "generateInsert": true,
        "grouping": [
          0,
          1
        ],
        "originDescription": null
      },
      "id": "org.apache.flink.streaming.api.operators.KeyedProcessOperator"
    })delimiter";

std::string ConfigStr_calc =
    R"delimiter({
    "name" : "Calc(select=[id, category])",
    "description":{
        "originDescription":null,
        "inputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"],
        "outputTypes":["BIGINT","BIGINT"],
        "indices":[
            {"exprType":"FIELD_REFERENCE","dataType":9,"colVal":0},
            {"exprType":"FIELD_REFERENCE","dataType":9,"colVal":1}],
        "condition":null
    },
        "id":"StreamExecCalc"
    })delimiter";

std::string ConfigStr_sink =
    R"delimiter({
    "name" : "blackHole sink",
    "description":{
        "inputTypes":["BIGINT","BIGINT","BIGINT","VARCHAR(2147483647)"]
    },
        "id":"org.apache.flink.table.runtime.operators.sink.SinkOperator"
    })delimiter";

omnistream::VectorBatch *getBatch1()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,       timestamp
    0     , 1000         , 1,      100
    */

    int rowCount = 1;

    auto vbatch = new omnistream::VectorBatch(rowCount);
    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);

    vKey->SetValue(0, 0);
    // vKey->SetValue(1, 0);
    // vKey->SetValue(2, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vWindowEndTimeLeft->SetValue(0, 1000);
    // vWindowEndTimeLeft->SetValue(1, 1001);
    // vWindowEndTimeLeft->SetValue(2, 1000);
    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vValLeft->SetValue(0, 1);
    // vValLeft->SetValue(0, 2);
    // vValLeft->SetValue(0, 3);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string1_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    // vbatch->setTimestamp(1, 101);
    // vbatch->setTimestamp(2, 102);
    return vbatch;
}

omnistream::VectorBatch *getBatch2()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp
    0     , 1000         , 0,     103
    0     , 1001         , 5,     104
    1     , 1000         , 6,     105
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 9);
    // vValLeft->SetValue(1, 5);
    // vValLeft->SetValue(2, 6);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string2_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 103);
    // vbatch->setTimestamp(1, 104);
    // vbatch->setTimestamp(2, 105);
    return vbatch;
}

omnistream::VectorBatch *getBatch3()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp
    0     , 1000         , 0,     103

    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 1);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 9);
    // vValLeft->SetValue(1, 5);
    // vValLeft->SetValue(2, 6);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string3_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 106);
    return vbatch;
}

omnistream::VectorBatch *getOutput1()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp  rowkind
    0     , 1000         , 0,     100      rowkind::insert 0
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 1);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    std::string str1 = "string1_" + std::to_string(0);
    std::string_view value1(str1.data(), str1.size());
    stringVecLeft->SetValue(0, value1);

    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    vbatch->setRowKind(0, RowKind::INSERT);

    return vbatch;
}

omnistream::VectorBatch *getOutput2()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     100     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 2;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vKey->SetValue(1, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 1);
    vValLeft->SetValue(1, 9);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    std::string str1 = "string1_" + std::to_string(0);
    std::string_view value1(str1.data(), str1.size());
    stringVecLeft->SetValue(0, value1);

    std::string str2 = "string2_" + std::to_string(0);
    std::string_view value2(str2.data(), str2.size());
    stringVecLeft->SetValue(1, value2);

    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    vbatch->setTimestamp(1, 103);
    // vbatch->setTimestamp(2, 105);
    vbatch->setRowKind(0, RowKind::UPDATE_BEFORE);
    vbatch->setRowKind(1, RowKind::UPDATE_AFTER);
    return vbatch;
}

omnistream::VectorBatch *getOutput3()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     100     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 1);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // Value Column
    auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    vValLeft->SetValue(0, 9);
    // vValLeft->SetValue(1, 5);
    // vValLeft->SetValue(2, 6);
    vbatch->Append(vValLeft);

    auto stringVecLeft =
        std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    for (int i = 0; i < rowCount; i++) {
        std::string str = "string3_" + std::to_string(i);
        std::string_view value(str.data(), str.size());
        stringVecLeft->SetValue(i, value);
    }
    vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 106);
    vbatch->setRowKind(0, RowKind::INSERT);
    return vbatch;
}

omnistream::VectorBatch *getOutput4()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp  rowkind
    0     , 1000         , 0,     103      rowkind::insert 0
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // // Value Column
    // auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    // vValLeft->SetValue(0, 1);
    // vbatch->Append(vValLeft);

    // auto stringVecLeft =
    //     std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    // std::string str1 = "string1_" + std::to_string(0);
    // std::string_view value1(str1.data(), str1.size());
    // stringVecLeft->SetValue(0, value1);

    // vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    vbatch->setRowKind(0, RowKind::INSERT);

    return vbatch;
}

omnistream::VectorBatch *getOutput5()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     100     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 2;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 0);
    vKey->SetValue(1, 0);
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    vWindowEndTimeLeft->SetValue(0, 1000);
    vWindowEndTimeLeft->SetValue(1, 1000);

    vbatch->Append(vWindowEndTimeLeft);

    // // Value Column
    // auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    // vValLeft->SetValue(0, 1);
    // vValLeft->SetValue(1, 9);
    // vbatch->Append(vValLeft);

    // auto stringVecLeft =
    //     std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);

    // std::string str1 = "string1_" + std::to_string(0);
    // std::string_view value1(str1.data(), str1.size());
    // stringVecLeft->SetValue(0, value1);

    // std::string str2 = "string2_" + std::to_string(0);
    // std::string_view value2(str2.data(), str2.size());
    // stringVecLeft->SetValue(1, value2);

    // vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 100);
    vbatch->setTimestamp(1, 103);
    // vbatch->setTimestamp(2, 105);
    vbatch->setRowKind(0, RowKind::UPDATE_BEFORE);
    vbatch->setRowKind(1, RowKind::UPDATE_AFTER);
    return vbatch;
}

omnistream::VectorBatch *getOutput6()
{
    // Batch
    /*
    (int) , (long)       , (long)
    KeyCol1, keyCol2, time,    timeStamp, rowkind
    0     , 1000         , 1,     100     rowkind::UPDATE_BEFORE
    0     , 1000         , 9,     103     RowKind::UPDATE_AFTER
    */

    int rowCount = 1;
    auto vbatch = new omnistream::VectorBatch(rowCount);

    // Key Column
    auto vKey = new omniruntime::vec::Vector<int64_t>(rowCount);
    vKey->SetValue(0, 1);
    // for (int j = 0; j < rowCount; j++) {
    //     vKey->SetValue(j, j);
    // }
    vbatch->Append(vKey);

    // WindowEndTimeColumn
    auto vWindowEndTimeLeft = new omniruntime::vec::Vector<int64_t>(rowCount);
    for (int j = 0; j < rowCount; j++) {
        vWindowEndTimeLeft->SetValue(j, 1000 + j);
    }

    vbatch->Append(vWindowEndTimeLeft);

    // // Value Column
    // auto vValLeft = new omniruntime::vec::Vector<int64_t>(rowCount);

    // vValLeft->SetValue(0, 9);
    // // vValLeft->SetValue(1, 5);
    // // vValLeft->SetValue(2, 6);
    // vbatch->Append(vValLeft);

    // auto stringVecLeft =
    //     std::make_unique<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>>(rowCount);
    // for (int i = 0; i < rowCount; i++) {
    //     std::string str = "string3_" + std::to_string(i);
    //     std::string_view value(str.data(), str.size());
    //     stringVecLeft->SetValue(i, value);
    // }
    // vbatch->Append(stringVecLeft.release());

    vbatch->setTimestamp(0, 106);
    vbatch->setRowKind(0, RowKind::INSERT);
    return vbatch;
}

template <typename T>
bool compareCol(omniruntime::vec::BaseVector *col1, omniruntime::vec::BaseVector *col2)
{
    if (col1->GetSize() != col2->GetSize()) {
        std::cout << "size wrong" << std::endl;
        return false;
    }

    if (col1->GetTypeId() != col2->GetTypeId()) {
        std::cout << "id wrong" << std::endl;
        return false;
    }

    omniruntime::vec::Vector<T> *col1cast = static_cast<omniruntime::vec::Vector<T> *>(col1);
    omniruntime::vec::Vector<T> *col2cast = static_cast<omniruntime::vec::Vector<T> *>(col2);

    if (col1cast->GetNullCount() != col2cast->GetNullCount()) {
        std::cout << "null count wrong" << std::endl;
        return false;
    }

    for (int i = 0; i < col1cast->GetSize(); i++) {
        auto c1v = col1cast->GetValue(i);
        auto c2v = col2cast->GetValue(i);
        auto c1null = col1cast->IsNull(i);
        auto c2null = col2cast->IsNull(i);
        if (c1null == c2null && c1null == true) {
            return true;
        }

        if (c1v != c2v) {
            std::cout << "value1 is: " << c1v << " value2 is:" << c2v << std::endl;
            return false;
        }
    }
    return true;
};

bool batchesEqual(omnistream::VectorBatch *b1, omnistream::VectorBatch *b2)
{
    if (b1->GetVectorCount() != b2->GetVectorCount()) {
        std::cout << "b1 count is: " << b1->GetVectorCount() << " b2 count is: " << b2->GetVectorCount() << std::endl;
        return false;
    }

    if (b1->GetRowCount() != b2->GetRowCount()) {
        return false;
    }

    // compare rowkind
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getRowKind(i) != (int)b2->getRowKind(i)) {
            std::cout << "rowkind wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    // compare timestamp
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getTimestamp(i) != (int)b2->getTimestamp(i)) {
            std::cout << "timestamp wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    return compareCol<int64_t>(b1->Get(0), b2->Get(0)) && compareCol<int64_t>(b1->Get(1), b2->Get(1)) &&
           compareCol<int64_t>(b1->Get(2), b2->Get(2)) &&
           compareCol<omniruntime::vec::LargeStringContainer<std::string_view>>(b1->Get(3), b2->Get(3));
};

bool batchesEqual2(omnistream::VectorBatch *b1, omnistream::VectorBatch *b2)
{
    if (b1->GetVectorCount() != b2->GetVectorCount()) {
        std::cout << "b1 count is: " << b1->GetVectorCount() << " b2 count is: " << b2->GetVectorCount() << std::endl;
        return false;
    }

    if (b1->GetRowCount() != b2->GetRowCount()) {
        return false;
    }

    // compare rowkind
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getRowKind(i) != (int)b2->getRowKind(i)) {
            std::cout << "rowkind wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    // compare timestamp
    for (int i = 0; i < b1->GetRowCount(); i++) {
        if ((int)b1->getTimestamp(i) != (int)b2->getTimestamp(i)) {
            std::cout << "timestamp wrong b1: " << (int)b1->getRowKind(i) << " b2: " << (int)b2->getRowKind(i)
                      << std::endl;
            return false;
        }
    }

    return compareCol<int64_t>(b1->Get(0), b2->Get(0)) && compareCol<int64_t>(b1->Get(1), b2->Get(1));
};

TEST(RowTimeDeduplicateTest, UpdateBeforeKeepLastRowTimeTest)
{
    auto inputVB1 = getBatch1();
    auto inputVB2 = getBatch2();
    auto inputVB3 = getBatch3();

    // VectorBatchUtil::printVectorBatch(inputVB2);

    json parsedJson = json::parse(description);
    OperatorConfig opConfig(uniqueName,                                // uniqueName:
        "Deduplicate(keep=[LastRow], key=[$1, $2], order=[ROWTIME])",  // Name
        parsedJson["operators"][0]["description"]["inputTypes"],
        parsedJson["operators"][0]["description"]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    BatchOutputTest *output = new BatchOutputTest();
    StreamOperatorFactory streamOperatorFactory;

    auto *keyedOp = dynamic_cast<KeyedProcessOperator<RowData *, omnistream::VectorBatch *, omnistream::VectorBatch *> *>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));

    // keyedOp->setup();

    // initiate keyedState
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(
        new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> *typeInfo = new std::vector<RowField>(
        {RowField("col0", BasicLogicalType::BIGINT), RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, *typeInfo));

    keyedOp->initializeState(initializer, ser);
    keyedOp->open();
    keyedOp->processBatch(new StreamRecord(inputVB1));

    // 状态后端为空，插入测试
    omnistream::VectorBatch *outputVB1_1 = reinterpret_cast<omnistream::VectorBatch *>(output->getVectorBatch());
    auto outputVB1 = getOutput1();
    EXPECT_TRUE(batchesEqual(outputVB1, outputVB1_1));

    keyedOp->processBatch(new StreamRecord(inputVB2));
    // 插入重复数据，回撤测试
    omnistream::VectorBatch *outputVB2_2 = reinterpret_cast<omnistream::VectorBatch *>(output->getVectorBatch());
    auto outputVB2 = getOutput2();
    EXPECT_TRUE(batchesEqual(outputVB2, outputVB2_2));

    // 状态后端有数据，插入测试
    keyedOp->processBatch(new StreamRecord(inputVB3));
    omnistream::VectorBatch *outputVB3_3 = reinterpret_cast<omnistream::VectorBatch *>(output->getVectorBatch());
    auto outputVB3 = getOutput3();
    EXPECT_TRUE(batchesEqual(outputVB3, outputVB3_3));

    std::cout << "finish test" << std::endl;

    delete inputVB1;
    delete inputVB2;
    delete inputVB3;
    delete outputVB1;
    delete outputVB2;
}