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
#include <algorithm>
#include <fstream>
#include <sstream>
#include <unordered_map>

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

int64_t longAt(omnistream::VectorBatch* batch, int col, int row)
{
    return reinterpret_cast<omniruntime::vec::Vector<int64_t>*>(batch->Get(col))->GetValue(row);
}

using FoldedRow = std::vector<int64_t>;

std::vector<FoldedRow> foldChangelog(omnistream::VectorBatch* batch, int columnCount)
{
    std::vector<FoldedRow> table;
    if (batch == nullptr) {
        return table;
    }
    for (int row = 0; row < batch->GetRowCount(); ++row) {
        FoldedRow values(columnCount);
        for (int col = 0; col < columnCount; ++col) {
            values[col] = longAt(batch, col, row);
        }
        RowKind kind = batch->getRowKind(row);
        if (kind == RowKind::INSERT || kind == RowKind::UPDATE_AFTER) {
            table.push_back(values);
        } else {
            for (auto it = table.rbegin(); it != table.rend(); ++it) {
                if (*it == values) {
                    table.erase(std::next(it).base());
                    break;
                }
            }
        }
    }
    std::sort(table.begin(), table.end());
    return table;
}

std::vector<FoldedRow> topNFinal(
    omnistream::VectorBatch* input, int partitionCol, int sortCol, int limit, bool withRank)
{
    struct Candidate {
        FoldedRow cols;
        int64_t sortValue = 0;
    };
    std::vector<int64_t> partitionOrder;
    std::unordered_map<int64_t, std::vector<Candidate>> groups;
    int columnCount = input->GetVectorCount();
    for (int row = 0; row < input->GetRowCount(); ++row) {
        Candidate candidate;
        candidate.cols.resize(columnCount);
        for (int col = 0; col < columnCount; ++col) {
            candidate.cols[col] = longAt(input, col, row);
        }
        candidate.sortValue = candidate.cols[sortCol];
        int64_t partition = candidate.cols[partitionCol];
        auto& top = groups[partition];
        if (top.empty()) {
            partitionOrder.push_back(partition);
        }
        if (static_cast<int>(top.size()) >= limit && candidate.sortValue <= top.back().sortValue) {
            continue;
        }
        auto pos = top.begin();
        while (pos != top.end() && candidate.sortValue <= pos->sortValue) {
            ++pos;
        }
        top.insert(pos, candidate);
        if (static_cast<int>(top.size()) > limit) {
            top.pop_back();
        }
    }
    std::vector<FoldedRow> expected;
    for (int64_t partition : partitionOrder) {
        const auto& top = groups[partition];
        for (size_t i = 0; i < top.size(); ++i) {
            FoldedRow row = top[i].cols;
            if (withRank) {
                row.push_back(static_cast<int64_t>(i) + 1);
            }
            expected.push_back(row);
        }
    }
    return expected;
}

void expectFolded(omnistream::VectorBatch* actual, const std::vector<FoldedRow>& expected)
{
    int columnCount = expected.empty() ? 0 : static_cast<int>(expected.front().size());
    if (actual != nullptr && actual->GetVectorCount() > 0) {
        columnCount = actual->GetVectorCount();
    }
    auto folded = foldChangelog(actual, columnCount);
    auto sortedExpected = expected;
    std::sort(sortedExpected.begin(), sortedExpected.end());
    ASSERT_EQ(folded.size(), sortedExpected.size());
    for (size_t i = 0; i < folded.size(); ++i) {
        EXPECT_EQ(folded[i], sortedExpected[i]) << "folded row " << i;
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
    // Folded top-3. Partition 0 keeps sort keys 5,4,3. Partition 1 keeps 11,10,9.
    omnistream::expectFolded(
        output->getVectorBatch(),
        {{0, 5, 7, 1}, {0, 4, 8, 2}, {0, 3, 9, 3}, {1, 11, 1, 1}, {1, 10, 2, 2}, {1, 9, 3, 3}});
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
    ASSERT_NE(inputVB, nullptr);
    auto expected = omnistream::topNFinal(inputVB, 0, 2, 10, true);
    op->processBatch(new StreamRecord(inputVB));
    omnistream::expectFolded(output->getVectorBatch(), expected);
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
    ASSERT_NE(inputVB, nullptr);
    auto expected = omnistream::topNFinal(inputVB, 1, 2, 3, false);
    op->processBatch(new StreamRecord(inputVB));
    omnistream::expectFolded(output->getVectorBatch(), expected);
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

    omnistream::expectFolded(output->getVectorBatch(), {{0, 30, 300}, {0, 20, 200}});
}

// Flink-aligned rank-number path: new higher sort key shifts previous ranks and drops the last.
TEST(AppendOnlyTopNFunctionTest, EvictBeyondTopNWithRowNumber)
{
    auto rankConfig = omnistream::makeRankConfig(true, true, 2);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* inputVB = omnistream::makeLongBatch({0, 0, 0}, {10, 20, 30}, {100, 200, 300});
    op->processBatch(new StreamRecord(inputVB));

    omnistream::expectFolded(output->getVectorBatch(), {{0, 30, 300, 1}, {0, 20, 200, 2}});
}

// Sort keys below the current Top-N threshold are ignored and must not change the changelog.
TEST(AppendOnlyTopNFunctionTest, IgnoreSortKeyOutsideTopN)
{
    auto rankConfig = omnistream::makeRankConfig(false, true, 2);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* first = omnistream::makeLongBatch({0, 0}, {20, 30}, {200, 300});
    op->processBatch(new StreamRecord(first));
    auto* afterFirst = output->getVectorBatch();
    ASSERT_NE(afterFirst, nullptr);
    const int rowsAfterFirst = afterFirst->GetRowCount();
    EXPECT_GT(rowsAfterFirst, 0);

    auto* second = omnistream::makeLongBatch({0}, {10}, {100});
    op->processBatch(new StreamRecord(second));

    // An ignored key emits no batch, so the previous changelog stays in place.
    auto* actual = output->getVectorBatch();
    EXPECT_EQ(actual, afterFirst);
    EXPECT_EQ(actual->GetRowCount(), rowsAfterFirst);
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
