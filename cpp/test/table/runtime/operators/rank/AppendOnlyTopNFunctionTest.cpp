#include <gtest/gtest.h>
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "vectorbatch/VectorBatch.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/RuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "typeutils/RowDataSerializer.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

namespace omnistream {
    VectorBatch *createVectorBatchFromCSV(std::string filepath, int nrow, int ncol)
    {
        VectorBatch* resultVB = new omnistream::VectorBatch(nrow);
        resultVB->ResizeVectorCount(ncol);
        for(int i = 0; i < ncol; i++) {
            auto vec = new omniruntime::vec::Vector<int64_t>(nrow);
            resultVB->SetVector(i, vec);
        }
        std::ifstream file(filepath);
        if (!file.is_open()) {
            std::cerr << "Error opening file\n";
            return nullptr;
        }
        std::string line;
        int irow = 0;
        while (std::getline(file, line)) {
            std::stringstream ss(line);
            std::string value;
            std::vector<std::string> row;
    
            while (std::getline(ss, value, ',')) {
                row.push_back(value);
            }
            for (size_t i = 0; i < row.size(); ++i) {
                reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(resultVB->GetVectors()[i])->SetValue(irow, std::stoll(row[i]));
            }
            irow++;
        }
        file.close();
        return resultVB;
    }
}

TEST(AppendOnlyTopNFunctionTest, OneLongPartitionKeyOneLongSortKey) {
    // partition by field0, sort desceding by field 1
    std::string description = R"DELIM({"originDescription":null,
"inputTypes":["BIGINT", "BIGINT", "BIGINT"],
"outputTypes":["BIGINT","BIGINT","BIGINT","BIGINT"],
"partitionKey":[0],
"outputRankNumber":true,
"rankRange":"rankStart=1, rankEnd=3",
"generateUpdateBefore":false,
"processFunction":"AppendOnlyTopNFunction",
"sortFieldIndices":[1],
"sortAscendingOrders":[false],
"sortNullsIsLast":[true]})DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    auto func = reinterpret_cast<KeyedProcessFunction<RowData*, RowData *, RowData *> *>(new AppendOnlyTopNFunction<RowData*>(
            rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest *output = new BatchOutputTest();
    auto *op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> typeInfo {RowField("col0", BasicLogicalType::BIGINT), RowField("col0", BasicLogicalType::BIGINT), RowField("col1", BasicLogicalType::BIGINT)};
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    int rowCnt = 12;
    std::vector<int64_t> col0(rowCnt);
    std::vector<int64_t> col1(rowCnt);
    std::vector<int64_t> col2(rowCnt);
    for (int i = 0; i < rowCnt; i++) {
        col0[i] = i < 6 ? 0 : 1;
        col1[i] = i;
        col2[i] = rowCnt - i;
    }
    auto vb = new omnistream::VectorBatch(rowCnt);
    auto vector0 = omniruntime::TestUtil::CreateVector(rowCnt, col0.data());
    auto vector1 = omniruntime::TestUtil::CreateVector(rowCnt, col1.data());
    auto vector2 = omniruntime::TestUtil::CreateVector(rowCnt, col2.data());
    vb->Append(vector0);
    vb->Append(vector1);
    vb->Append(vector2);
    op->processBatch(new StreamRecord(vb));
/*
+I      0       0       12      1
+U      0       1       11      1 // 1 > 0
+I      0       0       12      2 // prev row move one rank lower
+U      0       2       10      1 // 2 > 1
+U      0       1       11      2 // 1 moved one rank lower
+I      0       0       12      3 // 0 moved one rank lower
+U      0       3       9       1 // 3 > 2
+U      0       2       10      2 // 2 moved one rank lower
+U      0       1       11      3 // 1 moved one rank lower
+U      0       4       8       1 // 4 > 3
+U      0       3       9       2 // 3 moved one rank lower
+U      0       2       10      3 // 2 moved one rank lower
+U      0       5       7       1 // 5 > 4
+U      0       4       8       2 // 4 moved one rank lower
+U      0       3       9       3 // 3 moved one rank lower
+I      1       6       6       1 // new partition key 
+U      1       7       5       1
+I      1       6       6       2
+U      1       8       4       1
+U      1       7       5       2
+I      1       6       6       3
+U      1       9       3       1
+U      1       8       4       2
+U      1       7       5       3
+U      1       10      2       1
+U      1       9       3       2
+U      1       8       4       3
+U      1       11      1       1
+U      1       10      2       2
+U      1       9       3       3
 */
    std::vector<int64_t> expected0 = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
    std::vector<int64_t> expected1 = {0,1,0,2,1,0,3,2,1,4,3,2,5,4,3,6,7,6,8,7,6,9,8,7,10,9,8,11,10,9};
    std::vector<int64_t> expected2 = {12,11,12,10,11,12,9,10,11,8,9,10,7,8,9,6,5,6,4,5,6,3,4,5,2,3,4,1,2,3};
    std::vector<int64_t> expected3 = {1,1,2,1,2,3,1,2,3,1,2,3,1,2,3,1,1,2,1,2,3,1,2,3,1,2,3,1,2,3};

    auto expectedVB = new omnistream::VectorBatch(30);
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected0.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected1.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected2.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected3.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedVB);
    EXPECT_EQ(matched, true);

}

TEST(AppendOnlyTopNFunctionTest, Q19Top10) {
    std::string description = R"DELIM({"originDescription":null,
                                        "inputTypes":["BIGINT", "BIGINT", "BIGINT"],
                                        "outputTypes":["BIGINT","BIGINT","BIGINT","BIGINT"],
                                        "partitionKey":[0],
                                        "outputRankNumber":true,
                                        "rankRange":"rankStart=1, rankEnd=10",
                                        "generateUpdateBefore":true,
                                        "processFunction":"AppendOnlyTopNFunction",
                                        "sortFieldIndices":[2],
                                        "sortAscendingOrders":[false],
                                        "sortNullsIsLast":[true]})DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    auto func = reinterpret_cast<KeyedProcessFunction<long, RowData *, RowData *> *>(new AppendOnlyTopNFunction<long>(
            rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest *output = new BatchOutputTest();
    auto *op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> typeInfo {RowField("col0", BasicLogicalType::BIGINT), RowField("col1", BasicLogicalType::BIGINT), RowField("col2", BasicLogicalType::BIGINT)};
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto inputVB = createVectorBatchFromCSV("input/q19_input.csv", 150, 3);
    op->processBatch(new StreamRecord(inputVB));    
    auto expectedVB = createVectorBatchFromCSV("input/q19_expected_output.csv", 511, 4);
    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedVB);
}

TEST(AppendOnlyTopNFunctionTest, WithoutRowNumber) {
    std::string description = R"DELIM({"originDescription":null,
                                       "sortAscendingOrders": [false],
                                       "inputTypes": ["BIGINT","BIGINT","BIGINT"],
                                       "rankRange": "rankStart=1, rankEnd=3",
                                       "processFunction": "AppendOnlyTopNFunction",
                                       "sortFieldIndices": [2],
                                       "partitionKey": [1],
                                       "sortNullsIsLast": [true],
                                       "generateUpdateBefore": true,
                                       "outputTypes": ["BIGINT","BIGINT","BIGINT"],
                                       "originDescription": null,
                                       "outputRankNumber": false})DELIM";

    const nlohmann::json rankConfig = nlohmann::json::parse(description);
    auto func = reinterpret_cast<KeyedProcessFunction<long, RowData *, RowData *> *>(new AppendOnlyTopNFunction<long>(
            rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest *output = new BatchOutputTest();
    auto *op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    StreamTaskStateInitializerImpl *initializer = new StreamTaskStateInitializerImpl(new RuntimeEnvironment(new TaskInfoImpl("KeyedProcessOperatorTest", 2, 1, 0)));
    std::vector<RowField> typeInfo {RowField("col0", BasicLogicalType::BIGINT), RowField("col1", BasicLogicalType::BIGINT), RowField("col2", BasicLogicalType::BIGINT)};
    TypeSerializer *ser = new RowDataSerializer(new RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto inputVB = createVectorBatchFromCSV("input/q19_input.csv", 150, 3);
    op->processBatch(new StreamRecord(inputVB));
    auto expectedVB = createVectorBatchFromCSV("input/without_rownumber.csv", 59, 3);
    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedVB);
}
