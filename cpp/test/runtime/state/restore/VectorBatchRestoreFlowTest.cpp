/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreBackendDelegate.h"
#include "runtime/state/restore/RestoreKVState.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "runtime/state/restore/RestorePQState.h"
#include "runtime/state/restore/vb/VectorBatchRestoreFlow.h"

using namespace omnistream;

namespace {

// Mock RestoreKVState
class MockRestoreKVState : public RestoreKVState {
public:
    ~MockRestoreKVState() override = default;

    int flushCount = 0;
    int discardCount = 0;
    int lastKeyGroupId = -1;

    void flush() override
    {
        flushCount++;
    }
    void discard() override
    {
        discardCount++;
    }
    void setKeyGroupId(int keyGroupId) override
    {
        lastKeyGroupId = keyGroupId;
    }

protected:
    void writeLongEntry(const std::vector<int8_t>&, int64_t) override
    {
    }
    void writeBytesEntry(const std::vector<int8_t>&, ByteView) override
    {
    }
};

// Mock RestoreKVStateVB
class MockRestoreKVStateVB : public RestoreKVStateVB {
public:
    ~MockRestoreKVStateVB() override = default;

    int flushCount = 0;
    int discardCount = 0;
    int flushVBCount = 0;
    int resetBatchIdCount = 0;
    int lastKeyGroupId = -1;
    int appendRowCallCount = 0;

    ComboId appendRowToVectorBatch(const RowDataView&) override
    {
        appendRowCallCount++;
        return static_cast<ComboId>(appendRowCallCount - 1);
    }

    int getKeyGroupPrefixBytes() const override
    {
        return 0;
    }
    void resetBatchId() override
    {
        resetBatchIdCount++;
    }
    void setKeyGroupId(int keyGroupId) override
    {
        lastKeyGroupId = keyGroupId;
    }

protected:
    void flushVectorBatchIfNotEmpty() override
    {
        flushVBCount++;
    }
    void flushMainWriter() override
    {
    }
    void discardVectorBatch() override
    {
    }
    void discardMainWriter() override
    {
    }
    void writeLongEntry(const std::vector<int8_t>&, int64_t) override
    {
    }
    void writeBytesEntry(const std::vector<int8_t>&, ByteView) override
    {
    }
};

// Mock RestorePQState
class MockRestorePQState : public RestorePQState {
public:
    ~MockRestorePQState() override = default;

    int flushCount = 0;
    int discardCount = 0;

    void flush() override
    {
        flushCount++;
    }
    void discard() override
    {
        discardCount++;
    }
    void writeEntry(const std::vector<int8_t>&, const std::vector<int8_t>&) override
    {
    }
};

// Mock RestoreBackendDelegate
class MockRestoreBackendDelegate : public RestoreBackendDelegate {
public:
    ~MockRestoreBackendDelegate() override = default;

    int kvStateCreateCount = 0;
    int kvVbStateCreateCount = 0;
    int pqStateCreateCount = 0;

    std::unique_ptr<RestoreKVState> createKVState(int, const StateMetaInfoSnapshot&) override
    {
        kvStateCreateCount++;
        return std::make_unique<MockRestoreKVState>();
    }

    std::unique_ptr<RestoreKVStateVB> createKVStateVB(
        int, const StateMetaInfoSnapshot&, const std::vector<omniruntime::type::DataTypeId>&, int) override
    {
        kvVbStateCreateCount++;
        return std::make_unique<MockRestoreKVStateVB>();
    }

    std::unique_ptr<RestorePQState> createPQState(int, const StateMetaInfoSnapshot&) override
    {
        pqStateCreateCount++;
        return std::make_unique<MockRestorePQState>();
    }
};

// Mock Derived for VectorBatchRestoreFlow
class MockDerivedForFlow {
public:
    RestoreStateType getStateType(const StateMetaInfoSnapshot& metaInfo)
    {
        if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE)
            return RestoreStateType::PQ;
        if (metaInfo.getBackendStateType() == StateMetaInfoSnapshot::BackendStateType::KEY_VALUE) {
            if (metaInfo.getName() == "test-vb-state") return RestoreStateType::KV_WITH_VB;
            if (metaInfo.getName() == "test-transformed-state") return RestoreStateType::KV_TRANSFORMED;
            return RestoreStateType::KV;
        }
        return RestoreStateType::UNSUPPORT;
    }

    StateMetaInfoSnapshot buildOmniMainMetaInfo(int, const StateMetaInfoSnapshot& flinkMetaInfo)
    {
        return flinkMetaInfo;
    }

    void retrieveKVRowData(const std::vector<int8_t>&, const std::vector<int8_t>&, int, RestoreKVStateVB*)
    {
    }

    std::vector<omniruntime::type::DataTypeId> columnTypes(int)
    {
        return {omniruntime::type::DataTypeId::OMNI_LONG};
    }

    int batchSize(int)
    {
        return 1024;
    }
};

std::shared_ptr<StateMetaInfoSnapshot> makeSnapshot(
    const std::string& name, StateMetaInfoSnapshot::BackendStateType type, const std::string& stateType = "VALUE")
{
    std::unordered_map<std::string, std::string> options;
    options["KEYED_STATE_TYPE"] = stateType;
    return std::make_shared<StateMetaInfoSnapshot>(
        name, type, options, std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
}

} // namespace

TEST(VectorBatchRestoreFlowTest, CanCompileTemplate)
{
    MockDerivedForFlow derived;
    EXPECT_EQ(derived.batchSize(0), 1024);
    EXPECT_EQ(derived.columnTypes(0).size(), 1);
}

TEST(VectorBatchRestoreFlowTest, MockDerivedGetStateTypeReturnsCorrectTypes)
{
    MockDerivedForFlow derived;

    auto pqMeta = makeSnapshot("test-pq", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE);
    EXPECT_EQ(derived.getStateType(*pqMeta), RestoreStateType::PQ);

    auto kvMeta = makeSnapshot("test-kv", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
    EXPECT_EQ(derived.getStateType(*kvMeta), RestoreStateType::KV);

    auto kvVbMeta = makeSnapshot("test-vb-state", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
    EXPECT_EQ(derived.getStateType(*kvVbMeta), RestoreStateType::KV_WITH_VB);

    auto transformedMeta = makeSnapshot("test-transformed-state", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
    EXPECT_EQ(derived.getStateType(*transformedMeta), RestoreStateType::KV_TRANSFORMED);

    auto opMeta = makeSnapshot("test-op", StateMetaInfoSnapshot::BackendStateType::OPERATOR);
    EXPECT_EQ(derived.getStateType(*opMeta), RestoreStateType::UNSUPPORT);
}

TEST(VectorBatchRestoreFlowTest, MockBackendDelegateCreatesWriters)
{
    MockRestoreBackendDelegate delegate;
    auto meta = makeSnapshot("test", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);

    auto kvWriter = delegate.createKVState(0, *meta);
    EXPECT_NE(kvWriter, nullptr);

    std::vector<omniruntime::type::DataTypeId> columnTypes = {omniruntime::type::DataTypeId::OMNI_LONG};
    auto kvVbWriter = delegate.createKVStateVB(1, *meta, columnTypes, 1024);
    EXPECT_NE(kvVbWriter, nullptr);

    auto pqMeta = makeSnapshot("test-pq", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE);
    auto pqWriter = delegate.createPQState(2, *pqMeta);
    EXPECT_NE(pqWriter, nullptr);
}

TEST(VectorBatchRestoreFlowTest, MockKVStateFlushAndDiscard)
{
    MockRestoreKVState state;
    EXPECT_EQ(state.flushCount, 0);
    EXPECT_EQ(state.discardCount, 0);

    state.flush();
    EXPECT_EQ(state.flushCount, 1);

    state.discard();
    EXPECT_EQ(state.discardCount, 1);
}

TEST(VectorBatchRestoreFlowTest, MockKVStateVBFlushAndReset)
{
    MockRestoreKVStateVB state;
    EXPECT_EQ(state.flushVBCount, 0);
    EXPECT_EQ(state.resetBatchIdCount, 0);

    state.flushVB();
    EXPECT_EQ(state.flushVBCount, 1);

    state.resetBatchId();
    EXPECT_EQ(state.resetBatchIdCount, 1);
}

TEST(VectorBatchRestoreFlowTest, MockPQStateFlushAndDiscard)
{
    MockRestorePQState state;
    EXPECT_EQ(state.flushCount, 0);
    EXPECT_EQ(state.discardCount, 0);

    state.flush();
    EXPECT_EQ(state.flushCount, 1);

    state.discard();
    EXPECT_EQ(state.discardCount, 1);
}

TEST(VectorBatchRestoreFlowTest, StateMetaInfoSnapshotConstruction)
{
    auto meta = makeSnapshot("test-state", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "MAP");
    EXPECT_EQ(meta->getName(), "test-state");
    EXPECT_EQ(meta->getBackendStateType(), StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
}

TEST(VectorBatchRestoreFlowTest, MultipleStateMetaInfoSnapshots)
{
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas;
    metas.push_back(makeSnapshot("state1", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE));
    metas.push_back(makeSnapshot("state2", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE));
    metas.push_back(makeSnapshot("state3", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE, "MAP"));

    EXPECT_EQ(metas.size(), 3);
    EXPECT_EQ(metas[0]->getName(), "state1");
    EXPECT_EQ(metas[1]->getName(), "state2");
    EXPECT_EQ(metas[2]->getName(), "state3");
}

TEST(VectorBatchRestoreFlowTest, MockKVStateVBAppendRow)
{
    MockRestoreKVStateVB state;
    RowDataView row;
    std::vector<int8_t> valueBytes = {0x01, 0x02, 0x03};
    row.valueBytes = &valueBytes;

    ComboId combo1 = state.appendRowToVectorBatch(row);
    ComboId combo2 = state.appendRowToVectorBatch(row);

    EXPECT_EQ(combo1, 0);
    EXPECT_EQ(combo2, 1);
    EXPECT_EQ(state.appendRowCallCount, 2);
}

TEST(VectorBatchRestoreFlowTest, MockBackendDelegateWriterCount)
{
    MockRestoreBackendDelegate delegate;
    auto kvMeta = makeSnapshot("kv-state", StateMetaInfoSnapshot::BackendStateType::KEY_VALUE);
    auto pqMeta = makeSnapshot("pq-state", StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE);

    std::vector<omniruntime::type::DataTypeId> columnTypes = {omniruntime::type::DataTypeId::OMNI_LONG};

    delegate.createKVState(0, *kvMeta);
    delegate.createKVState(1, *kvMeta);
    delegate.createKVStateVB(2, *kvMeta, columnTypes, 1024);
    delegate.createPQState(3, *pqMeta);

    EXPECT_EQ(delegate.kvStateCreateCount, 2);
    EXPECT_EQ(delegate.kvVbStateCreateCount, 1);
    EXPECT_EQ(delegate.pqStateCreateCount, 1);
}
