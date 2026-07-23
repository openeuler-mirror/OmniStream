/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Description: unit tests for streaming semi/anti join (EXISTS/NOT EXISTS), append-only inputs.

#include <gtest/gtest.h>
#include <memory>
#include "table/runtime/operators/join/StreamingSemiAntiJoinOperator.h"
#include "runtime/taskmanager/OmniRuntimeEnvironment.h"
#include "test/core/operators/OutputTest.h"
#include "core/typeutils/LongSerializer.h"

// Left: (BIGINT key, BIGINT value); Right: (BIGINT key); output: left-only (key, value).
static std::string semiDesc = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["BIGINT", "BIGINT"],
  "rightInputTypes": ["BIGINT"],
  "outputTypes": ["BIGINT", "BIGINT"],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "nonEquiCondition": null,
  "joinType": "LeftSemiJoin",
  "filterNulls": [true],
  "leftInputSpec": "NoUniqueKey",
  "rightInputSpec": "NoUniqueKey",
  "leftUniqueKeys": [],
  "rightUniqueKeys": []
})delimiter";

static std::string antiDesc = R"delimiter({
  "originDescription": null,
  "leftInputTypes": ["BIGINT", "BIGINT"],
  "rightInputTypes": ["BIGINT"],
  "outputTypes": ["BIGINT", "BIGINT"],
  "leftJoinKey": [0],
  "rightJoinKey": [0],
  "nonEquiCondition": null,
  "joinType": "LeftAntiJoin",
  "filterNulls": [true],
  "leftInputSpec": "NoUniqueKey",
  "rightInputSpec": "NoUniqueKey",
  "leftUniqueKeys": [],
  "rightUniqueKeys": []
})delimiter";

using namespace omnistream;

// Left rows: (1, 10), (2, 20).
static omnistream::VectorBatch* getLeftBatch()
{
    auto vb = new omnistream::VectorBatch(2);
    auto key = new omniruntime::vec::Vector<int64_t>(2);
    key->SetValue(0, 1);
    key->SetValue(1, 2);
    vb->Append(key);
    auto val = new omniruntime::vec::Vector<int64_t>(2);
    val->SetValue(0, 10);
    val->SetValue(1, 20);
    vb->Append(val);
    for (int i = 0; i < 2; i++) {
        vb->setRowKind(i, RowKind::INSERT);
    }
    return vb;
}

// Right rows: single row with given key.
static omnistream::VectorBatch* getRightBatch(int64_t key)
{
    auto vb = new omnistream::VectorBatch(1);
    auto k = new omniruntime::vec::Vector<int64_t>(1);
    k->SetValue(0, key);
    vb->Append(k);
    vb->setRowKind(0, RowKind::INSERT);
    return vb;
}

static bool colEqualsInt64(omniruntime::vec::BaseVector* col, std::vector<int64_t> expected)
{
    auto c = static_cast<omniruntime::vec::Vector<int64_t>*>(col);
    if (c->GetSize() != (int)expected.size()) {
        return false;
    }
    for (size_t i = 0; i < expected.size(); i++) {
        if (c->IsNull(i) || c->GetValue(i) != expected[i]) {
            return false;
        }
    }
    return true;
}

// Build operator + runtime env; returns objects via params. Caller frees op/initializer/out.
static StreamingSemiAntiJoinOperator<long>* makeOp(const std::string& desc, OutputTestVectorBatch* out)
{
    nlohmann::json parsed = nlohmann::json::parse(desc);
    auto* op = new StreamingSemiAntiJoinOperator<long>(parsed, out);
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
    op->setup();
    op->initializeState(initializer, new LongSerializer());
    op->open();
    return op;
}

// SEMI: left arrives first (no right match) -> no output; right arrives -> emit matching left row once;
// a duplicate right row -> no output (dedup via numAssociate 0->1).
TEST(StreamingSemiAntiJoinOperatorTest, SemiEmitOnRightArrivalAndDedup)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(semiDesc, out);

    op->processBatch1(new StreamRecord(getLeftBatch()));   // right empty -> no emit
    EXPECT_EQ(out->getAll().size(), 0u);

    op->processBatch2(new StreamRecord(getRightBatch(1))); // left (1,10) 0->1 -> emit +I(1,10)
    ASSERT_EQ(out->getAll().size(), 1u);
    ASSERT_EQ(out->getAll()[0]->GetRowCount(), 1);
    EXPECT_TRUE(colEqualsInt64(out->getAll()[0]->Get(0), {1}));
    EXPECT_TRUE(colEqualsInt64(out->getAll()[0]->Get(1), {10}));
    EXPECT_EQ(out->getAll()[0]->getRowKind(0), RowKind::INSERT);

    op->processBatch2(new StreamRecord(getRightBatch(1))); // left (1,10) numAssociate>0 -> no emit (dedup)
    EXPECT_EQ(out->getAll().size(), 1u);

    op->close();
    delete op;
    delete out;
}

// SEMI: right arrives first; left arrives with an existing right match -> emit on left arrival.
TEST(StreamingSemiAntiJoinOperatorTest, SemiEmitOnLeftArrivalWithExistingRight)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(semiDesc, out);

    op->processBatch2(new StreamRecord(getRightBatch(1))); // left empty -> no emit
    EXPECT_EQ(out->getAll().size(), 0u);

    op->processBatch1(new StreamRecord(getLeftBatch()));   // (1,10) has match -> +I; (2,20) no match -> none
    ASSERT_EQ(out->getAll().size(), 1u);
    ASSERT_EQ(out->getAll()[0]->GetRowCount(), 1);
    EXPECT_TRUE(colEqualsInt64(out->getAll()[0]->Get(0), {1}));
    EXPECT_TRUE(colEqualsInt64(out->getAll()[0]->Get(1), {10}));

    op->close();
    delete op;
    delete out;
}

// ANTI: left arrives with no right match -> emit +I both; right late match -> retract -D the matched left row.
TEST(StreamingSemiAntiJoinOperatorTest, AntiEmitOnLeftAndRetractOnRightLateMatch)
{
    auto* out = new OutputTestVectorBatch();
    auto* op = makeOp(antiDesc, out);

    op->processBatch1(new StreamRecord(getLeftBatch()));   // right empty -> ANTI emits both as +I
    ASSERT_EQ(out->getAll().size(), 1u);
    ASSERT_EQ(out->getAll()[0]->GetRowCount(), 2);
    EXPECT_TRUE(colEqualsInt64(out->getAll()[0]->Get(0), {1, 2}));
    EXPECT_TRUE(colEqualsInt64(out->getAll()[0]->Get(1), {10, 20}));

    op->processBatch2(new StreamRecord(getRightBatch(1))); // left (1,10) 0->1 -> retract -D(1,10)
    ASSERT_EQ(out->getAll().size(), 2u);
    ASSERT_EQ(out->getAll()[1]->GetRowCount(), 1);
    EXPECT_TRUE(colEqualsInt64(out->getAll()[1]->Get(0), {1}));
    EXPECT_TRUE(colEqualsInt64(out->getAll()[1]->Get(1), {10}));
    EXPECT_EQ(out->getAll()[1]->getRowKind(0), RowKind::DELETE);

    op->close();
    delete op;
    delete out;
}
