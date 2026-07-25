/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * Operator-level tests for native JSON_OBJECTAGG / JSON_ARRAYAGG, driven end-to-end through
 * GroupAggFunction::processBatch (exercises open(store) + accumulate(VectorBatch) + getValue).
 */
#include "table/runtime/operators/aggregate/GroupAggFunction.h"
#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include "streaming/runtime/streamrecord/StreamRecord.h"
#include "streaming/api/operators/StreamOperatorFactory.h"
#include "streaming/api/operators/KeyedProcessOperator.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/RowDataSerializer.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "core/api/common/TaskInfoImpl.h"
#include "core/operators/OutputTest.h"

using namespace omnistream;
using namespace omnistream::datastream;

namespace {

// Build a keyed operator for the given description and wire up a HashMap (heap) keyed state backend.
KeyedProcessOperator<RowData*, RowData*, RowData*>* buildKeyedOp(const std::string& description, BatchOutputTest* output)
{
    nlohmann::json parsedJson = nlohmann::json::parse(description);
    omnistream::OperatorConfig opConfig(
        "org.apache.flink.streaming.api.operators.KeyedProcessOperator",
        "Group_By_Json",
        parsedJson["operators"][0]["inputTypes"],
        parsedJson["operators"][0]["outputTypes"],
        parsedJson["operators"][0]["description"]);

    StreamOperatorFactory streamOperatorFactory;
    auto* keyedOp = dynamic_cast<KeyedProcessOperator<RowData*, RowData*, RowData*>*>(
        streamOperatorFactory.createOperatorAndCollector(opConfig, output));

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
    auto* initializer = new StreamTaskStateInitializerImpl(env2);
    auto* typeInfo = new std::vector<omnistream::RowField>(
        {omnistream::RowField("col0", BasicLogicalType::BIGINT),
         omnistream::RowField("col1", BasicLogicalType::BIGINT)});
    TypeSerializer* ser = new RowDataSerializer(new omnistream::RowType(false, *typeInfo));
    keyedOp->initializeState(initializer, ser);
    keyedOp->open();
    return keyedOp;
}

std::string readVarchar(omnistream::VectorBatch* batch, int col, int row)
{
    auto value =
        reinterpret_cast<omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>*>(
            batch->Get(col))
            ->GetValue(row);
    return std::string(value);
}

} // namespace

// JSON_OBJECTAGG: single group, three key/value pairs, VARCHAR values -> sorted JSON object.
TEST(JsonAggFunctionTest, ObjectAggSingleGroupVarcharValues)
{
    // accTypes carries the RAW MapView accumulator (mirrors the real Flink plan). GroupAggFunction
    // filters RAW entries out, so accumulatorArity == 0 and the JSON handler occupies no acc slot.
    // The value column (col2) already holds the JSON_STRING output (quoted for strings), matching the
    // planner's WrapJsonAggFunctionArgumentsRule; the handler stores it verbatim.
    std::string description = R"DELIM({"input_channels":[0],
        "operators":[{"description":{
            "aggInfoList":{"accTypes":["*org.apache.flink.table.runtime.functions.aggregate.JsonObjectAggFunction$Accumulator<`map` RAW('org.apache.flink.table.api.dataview.MapView', '...')>*"],"aggValueTypes":["VARCHAR(2147483647)"],
                "aggregateCalls":[{"aggregationFunction":"JsonObjectAggFunction","argIndexes":[1,2],"consumeRetraction":"false","filterArg":-1,"name":"JSON_OBJECTAGG_NULL_ON_NULL($1, $2)"}],
                "indexOfCountStar":-1},
            "grouping":[0],
            "distinctInfos":[],
            "inputTypes":["BIGINT","VARCHAR(2147483647)","VARCHAR(2147483647)"],
            "outputTypes":["BIGINT","VARCHAR(2147483647)"]},
            "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
            "name":"GroupAggregate[3]"}],
        "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    BatchOutputTest* output = new BatchOutputTest();
    auto* keyedOp = buildKeyedOp(description, output);

    // Build input batch: col0 = group key (BIGINT), col1 = json key (VARCHAR), col2 = JSON_STRING(value)
    // i.e. the already-serialized value fragment (strings are quoted: "z"/"x"/"y").
    // Insertion order is deliberately unsorted (c, a, b) to verify deterministic sorted output.
    const int n = 3;
    auto* vbatch = new omnistream::VectorBatch(n);
    auto* keyCol = new omniruntime::vec::Vector<int64_t>(n);
    auto* jsonKeyCol = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(n);
    auto* valCol = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(n);
    std::array<std::string_view, n> jsonKeys = {"c", "a", "b"};
    std::array<std::string_view, n> values = {"\"z\"", "\"x\"", "\"y\""};
    for (int i = 0; i < n; ++i) {
        keyCol->SetValue(i, 100L);
        jsonKeyCol->SetValue(i, jsonKeys[i]);
        valCol->SetValue(i, values[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(keyCol);
    vbatch->Append(jsonKeyCol);
    vbatch->Append(valCol);

    keyedOp->processBatch(new StreamRecord(vbatch));
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    ASSERT_EQ(resultBatch->GetRowCount(), 1);
    EXPECT_EQ(resultBatch->GetValueAt<int64_t>(0, 0), 100L);
    EXPECT_EQ(readVarchar(resultBatch, 1, 0), R"({"a":"x","b":"y","c":"z"})");
}

// JSON_ARRAYAGG: single group, three items -> JSON array in insertion order.
TEST(JsonAggFunctionTest, ArrayAggSingleGroupVarcharItems)
{
    // accTypes carries the RAW ListView accumulator (mirrors the real Flink plan) -> filtered out ->
    // accumulatorArity == 0. The item column already holds the JSON_STRING output (quoted strings).
    std::string description = R"DELIM({"input_channels":[0],
        "operators":[{"description":{
            "aggInfoList":{"accTypes":["*org.apache.flink.table.runtime.functions.aggregate.JsonArrayAggFunction$Accumulator<`list` RAW('org.apache.flink.table.api.dataview.ListView', '...')>*"],"aggValueTypes":["VARCHAR(2147483647)"],
                "aggregateCalls":[{"aggregationFunction":"JsonArrayAggFunction","argIndexes":[1],"consumeRetraction":"false","filterArg":-1,"name":"JSON_ARRAYAGG_ABSENT_ON_NULL($1)"}],
                "indexOfCountStar":-1},
            "grouping":[0],
            "distinctInfos":[],
            "inputTypes":["BIGINT","VARCHAR(2147483647)"],
            "outputTypes":["BIGINT","VARCHAR(2147483647)"]},
            "id":"org.apache.flink.streaming.api.operators.KeyedProcessOperator",
            "name":"GroupAggregate[3]"}],
        "partition":{"channelNumber":1,"partitionName":"forward"}})DELIM";

    BatchOutputTest* output = new BatchOutputTest();
    auto* keyedOp = buildKeyedOp(description, output);

    const int n = 3;
    auto* vbatch = new omnistream::VectorBatch(n);
    auto* keyCol = new omniruntime::vec::Vector<int64_t>(n);
    auto* itemCol = new omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>(n);
    // JSON_STRING output for each item (strings are quoted).
    std::array<std::string_view, n> items = {"\"a\"", "\"b\"", "\"c\""};
    for (int i = 0; i < n; ++i) {
        keyCol->SetValue(i, 7L);
        itemCol->SetValue(i, items[i]);
        vbatch->setRowKind(i, RowKind::INSERT);
    }
    vbatch->Append(keyCol);
    vbatch->Append(itemCol);

    keyedOp->processBatch(new StreamRecord(vbatch));
    auto* resultBatch = reinterpret_cast<omnistream::VectorBatch*>(output->getVectorBatch());
    ASSERT_EQ(resultBatch->GetRowCount(), 1);
    EXPECT_EQ(resultBatch->GetValueAt<int64_t>(0, 0), 7L);
    EXPECT_EQ(readVarchar(resultBatch, 1, 0), R"(["a","b","c"])");
}
