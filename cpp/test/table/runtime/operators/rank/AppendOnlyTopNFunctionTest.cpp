#include <gtest/gtest.h>
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "typeutils/RowDataSerializer.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/data/RowKind.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>
#include <fstream>
#include <sstream>

namespace omnistream {
VectorBatch* createVectorBatchFromCSV(std::string filepath, int nrow, int ncol)
{
    VectorBatch* resultVB = new omnistream::VectorBatch(nrow);
    resultVB->ResizeVectorCount(ncol);
    for (int i = 0; i < ncol; i++) {
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
            reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(resultVB->GetVectors()[i])
                ->SetValue(irow, std::stoll(row[i]));
        }
        irow++;
    }
    file.close();
    return resultVB;
}

nlohmann::json makeRankConfig(bool outputRankNumber, bool generateUpdateBefore, int rankEnd)
{
    std::string outputTypes =
        outputRankNumber ? R"("BIGINT","BIGINT","BIGINT","BIGINT")" : R"("BIGINT","BIGINT","BIGINT")";
    std::string json =
        std::string(R"({"originDescription":null,"inputTypes":["BIGINT","BIGINT","BIGINT"],"outputTypes":[)") +
        outputTypes + R"(],"partitionKey":[0],"outputRankNumber":)" + (outputRankNumber ? "true" : "false") +
        R"(,"rankRange":"rankStart=1, rankEnd=)" + std::to_string(rankEnd) + R"(","generateUpdateBefore":)" +
        (generateUpdateBefore ? "true" : "false") +
        R"(,"processFunction":"AppendOnlyTopNFunction","sortFieldIndices":[1],"sortAscendingOrders":[false],"sortNullsIsLast":[true]})";
    return nlohmann::json::parse(json);
}

KeyedProcessOperator<RowData*, RowData*, RowData*>* openAppendOnlyTopN(
    const nlohmann::json& rankConfig, BatchOutputTest* output)
{
    auto* topN = new AppendOnlyTopNFunction<RowData*>(rankConfig);
    auto func = reinterpret_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(topN);
    nlohmann::json newRankConfig = rankConfig;
    auto* op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    TypeSerializer* ser = new BinaryRowDataSerializer(1);
    op->initializeState(initializer, ser);
    op->open();
    return op;
}

omnistream::VectorBatch* makeLongBatch(
    const std::vector<int64_t>& col0, const std::vector<int64_t>& col1, const std::vector<int64_t>& col2)
{
    int rowCnt = static_cast<int>(col0.size());
    auto* vb = new omnistream::VectorBatch(rowCnt);
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(col0.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(col1.data())));
    vb->Append(omniruntime::TestUtil::CreateVector(rowCnt, const_cast<int64_t*>(col2.data())));
    return vb;
}

void expectRowKinds(omnistream::VectorBatch* actual, const std::vector<RowKind>& expected)
{
    ASSERT_NE(actual, nullptr);
    ASSERT_EQ(actual->GetRowCount(), static_cast<int32_t>(expected.size()));
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(actual->getRowKind(i), expected[i]) << "rowKind mismatch at " << i;
    }
}

} // namespace omnistream

TEST(AppendOnlyTopNFunctionTest, OneLongPartitionKeyOneLongSortKey)
{
    // State is Flink-aligned MapState<sortKey, List<RowData>> named data-state-with-append.
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
    auto func = reinterpret_cast<KeyedProcessFunction<RowData*, RowData*, RowData*>*>(
        new AppendOnlyTopNFunction<RowData*>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest* output = new BatchOutputTest();
    auto* op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    TypeSerializer* ser = new BinaryRowDataSerializer(1);
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
    std::vector<int64_t> expected0 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    std::vector<int64_t> expected1 = {0, 1, 0, 2, 1, 0, 3, 2, 1, 4,  3, 2, 5,  4,  3,
                                      6, 7, 6, 8, 7, 6, 9, 8, 7, 10, 9, 8, 11, 10, 9};
    std::vector<int64_t> expected2 = {12, 11, 12, 10, 11, 12, 9, 10, 11, 8, 9, 10, 7, 8, 9,
                                      6,  5,  6,  4,  5,  6,  3, 4,  5,  2, 3, 4,  1, 2, 3};
    std::vector<int64_t> expected3 = {1, 1, 2, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3,
                                      1, 1, 2, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3};

    auto expectedVB = new omnistream::VectorBatch(30);
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected0.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected1.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected2.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(30, expected3.data()));
    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedVB);
    EXPECT_EQ(matched, true);
}

TEST(AppendOnlyTopNFunctionTest, Q19Top10)
{
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
    auto func =
        reinterpret_cast<KeyedProcessFunction<long, RowData*, RowData*>*>(new AppendOnlyTopNFunction<long>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest* output = new BatchOutputTest();
    auto* op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto inputVB = createVectorBatchFromCSV("input/q19_input.csv", 150, 3);
    op->processBatch(new StreamRecord(inputVB));
    auto expectedVB = createVectorBatchFromCSV("input/q19_expected_output.csv", 511, 4);
    ASSERT_NE(inputVB, nullptr);
    ASSERT_NE(expectedVB, nullptr);
    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedVB);
    EXPECT_EQ(matched, true);
}

TEST(AppendOnlyTopNFunctionTest, WithoutRowNumber)
{
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
    auto func =
        reinterpret_cast<KeyedProcessFunction<long, RowData*, RowData*>*>(new AppendOnlyTopNFunction<long>(rankConfig));

    nlohmann::json newRankConfig = rankConfig;
    BatchOutputTest* output = new BatchOutputTest();
    auto* op = new KeyedProcessOperator(func, output, newRankConfig);
    op->setup();
    auto env2 = new omnistream::RuntimeEnvironmentV2();
    auto taskInfo = new TaskInformationPOD();
    taskInfo->setStateBackend("HashMapStateBackend");
    {
        auto configPOD = taskInfo->getStreamConfigPOD();
        auto operatorDesc = configPOD.getOperatorDescription();
        operatorDesc.setOperatorId("deadbeefdeadbeefdeadbeefdeadbeef");
        configPOD.setOperatorDescription(operatorDesc);
        taskInfo->setStreamConfigPOD(configPOD);
    }
    env2->SetTaskStateManager(std::make_shared<omnistream::TaskStateManager>());
    env2->setTaskConfiguration(*taskInfo);
    StreamTaskStateInitializerImpl* initializer = new StreamTaskStateInitializerImpl(env2);
    std::vector<omnistream::RowField> typeInfo{
        omnistream::RowField("col0", BasicLogicalType::BIGINT),
        omnistream::RowField("col1", BasicLogicalType::BIGINT),
        omnistream::RowField("col2", BasicLogicalType::BIGINT)};
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, typeInfo));
    op->initializeState(initializer, ser);
    EXPECT_NO_THROW(op->open());

    auto inputVB = createVectorBatchFromCSV("input/q19_input.csv", 150, 3);
    op->processBatch(new StreamRecord(inputVB));
    auto expectedVB = createVectorBatchFromCSV("input/without_rownumber.csv", 59, 3);
    ASSERT_NE(inputVB, nullptr);
    ASSERT_NE(expectedVB, nullptr);
    bool matched = omniruntime::TestUtil::VecBatchMatch(output->getVectorBatch(), expectedVB);
    EXPECT_EQ(matched, true);
}

// Flink-aligned eviction: descending Top-2 without rank number emits DELETE for the dropped row.
TEST(AppendOnlyTopNFunctionTest, EvictBeyondTopNWithoutRowNumber)
{
    auto rankConfig = omnistream::makeRankConfig(false, true, 2);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    // sort key is col1 DESC: 10, then 20, then 30 evicts 10.
    auto* inputVB = omnistream::makeLongBatch({0, 0, 0}, {10, 20, 30}, {100, 200, 300});
    op->processBatch(new StreamRecord(inputVB));

    std::vector<int64_t> expected0 = {0, 0, 0, 0};
    std::vector<int64_t> expected1 = {10, 20, 10, 30};
    std::vector<int64_t> expected2 = {100, 200, 100, 300};
    auto* expectedVB = omnistream::makeLongBatch(expected0, expected1, expected2);
    auto* actual = output->getVectorBatch();
    omnistream::expectRowKinds(actual, {RowKind::INSERT, RowKind::INSERT, RowKind::DELETE, RowKind::INSERT});
    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(actual, expectedVB));
}

// Flink-aligned rank-number path: new higher sort key shifts previous ranks and drops the last.
TEST(AppendOnlyTopNFunctionTest, EvictBeyondTopNWithRowNumber)
{
    auto rankConfig = omnistream::makeRankConfig(true, true, 2);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* inputVB = omnistream::makeLongBatch({0, 0, 0}, {10, 20, 30}, {100, 200, 300});
    op->processBatch(new StreamRecord(inputVB));

    // +I 10 r1
    // -U 10 r1, +U 20 r1, +I 10 r2
    // -U 20 r1, +U 30 r1, -U 10 r2, +U 20 r2
    std::vector<int64_t> expected0 = {0, 0, 0, 0, 0, 0, 0, 0};
    std::vector<int64_t> expected1 = {10, 10, 20, 10, 20, 30, 10, 20};
    std::vector<int64_t> expected2 = {100, 100, 200, 100, 200, 300, 100, 200};
    std::vector<int64_t> expected3 = {1, 1, 1, 2, 1, 1, 2, 2};
    auto* expectedVB = new omnistream::VectorBatch(8);
    expectedVB->Append(omniruntime::TestUtil::CreateVector(8, expected0.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(8, expected1.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(8, expected2.data()));
    expectedVB->Append(omniruntime::TestUtil::CreateVector(8, expected3.data()));

    auto* actual = output->getVectorBatch();
    omnistream::expectRowKinds(
        actual,
        {RowKind::INSERT,
         RowKind::UPDATE_BEFORE,
         RowKind::UPDATE_AFTER,
         RowKind::INSERT,
         RowKind::UPDATE_BEFORE,
         RowKind::UPDATE_AFTER,
         RowKind::UPDATE_BEFORE,
         RowKind::UPDATE_AFTER});
    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(actual, expectedVB));
}

// Sort keys below the current Top-N threshold are ignored and must not change the changelog.
TEST(AppendOnlyTopNFunctionTest, IgnoreSortKeyOutsideTopN)
{
    auto rankConfig = omnistream::makeRankConfig(false, true, 2);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* first = omnistream::makeLongBatch({0, 0}, {20, 30}, {200, 300});
    op->processBatch(new StreamRecord(first));
    auto* second = omnistream::makeLongBatch({0}, {10}, {100});
    op->processBatch(new StreamRecord(second));

    auto* actual = output->getVectorBatch();
    ASSERT_NE(actual, nullptr);
    EXPECT_EQ(actual->GetRowCount(), 0);
}

// Three rows share one sort key and each stays inside Top-N, so the changelog keeps all three inserts.
TEST(AppendOnlyTopNFunctionTest, SameSortKeyInOneBatchKeepsAllInserts)
{
    auto rankConfig = omnistream::makeRankConfig(false, false, 10);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* inputVB = omnistream::makeLongBatch({0, 0, 0}, {10, 10, 10}, {1, 2, 3});
    op->processBatch(new StreamRecord(inputVB));

    auto* expectedVB = omnistream::makeLongBatch({0, 0, 0}, {10, 10, 10}, {1, 2, 3});
    auto* actual = output->getVectorBatch();
    omnistream::expectRowKinds(actual, {RowKind::INSERT, RowKind::INSERT, RowKind::INSERT});
    EXPECT_TRUE(omniruntime::TestUtil::VecBatchMatch(actual, expectedVB));
}
