#include <gtest/gtest.h>
#include <map>
#include "table/runtime/operators/rank/AppendOnlyTopNFunction.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/data/vectorbatch/VectorBatch.h"
#include "streaming/api/functions/KeyedProcessFunction.h"
#include "api/operators/KeyedProcessOperator.h"
#include "core/operators/OutputTest.h"
#include "taskmanager/OmniRuntimeEnvironment.h"
#include "api/common/TaskInfoImpl.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/data/RowKind.h"
#include "OmniOperatorJIT/core/test/util/test_util.h"
#include <nlohmann/json.hpp>

namespace omnistream {
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

// Sort keys below the current Top-N threshold are ignored and produce no extra output.
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

    // An ignored key emits no batch, so the previous output stays in place.
    auto* actual = output->getVectorBatch();
    EXPECT_EQ(actual, afterFirst);
    EXPECT_EQ(actual->GetRowCount(), rowsAfterFirst);
}

// Three rows share one sort key and each stays inside Top-N, so the output keeps all three inserts.
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

// Flink AppendOnlyTopN data-state-with-append is MapState<sortKey, List<input row>>.
// Only sort keys inside the rank range are stored, and equal sort keys share one list.
TEST(AppendOnlyTopNFunctionTest, DataStateWithAppendMatchesFlinkSortKeyMap)
{
    auto rankConfig = omnistream::makeRankConfig(false, false, 10);
    auto* output = new BatchOutputTest();
    auto* op = omnistream::openAppendOnlyTopN(rankConfig, output);

    auto* inputVB = omnistream::makeLongBatch({0, 0, 0}, {30, 20, 20}, {300, 200, 201});
    op->processBatch(new StreamRecord(inputVB));
    EXPECT_TRUE(true);

    auto* partitionKey = BinaryRowData::createBinaryRowDataWithMem(1);
    int64_t partitionValue = 0;
    partitionKey->setLong(0, partitionValue);
    op->setCurrentKey(partitionKey);

    auto* heap = dynamic_cast<HeapKeyedStateBackend<RowData*>*>(op->getKeyedStateBackend());
    ASSERT_NE(heap, nullptr);
    using RankMap = emhash7::HashMap<RowData*, std::vector<RowData*>*>;
    auto* table = reinterpret_cast<CopyOnWriteStateTable<RowData*, VoidNamespace, RankMap*>*>(
        heap->getStateTablePtr("data-state-with-append"));
    auto* entries = table->get(VoidNamespace());
    ASSERT_NE(entries, nullptr);

    std::map<int64_t, std::vector<int64_t>> payloadBySortKey;
    for (const auto& entry : *entries) {
        ASSERT_NE(entry.first, nullptr);
        ASSERT_FALSE(entry.first->isNullAt(0));
        ASSERT_NE(entry.second, nullptr);
        std::vector<int64_t> payloads;
        for (RowData* row : *entry.second) {
            ASSERT_NE(row, nullptr);
            EXPECT_EQ(*row->getLong(1), *entry.first->getLong(0));
            payloads.push_back(*row->getLong(2));
        }
        payloadBySortKey.emplace(*entry.first->getLong(0), payloads);
    }
    EXPECT_EQ(payloadBySortKey.size(), 2u);
    EXPECT_EQ(payloadBySortKey[30], std::vector<int64_t>{300});
    EXPECT_EQ(payloadBySortKey[20], (std::vector<int64_t>{200, 201}));
}
