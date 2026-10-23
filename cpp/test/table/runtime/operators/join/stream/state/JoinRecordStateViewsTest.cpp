/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "table/runtime/operators/join/stream/state/JoinRecordStateViews.h"

#include <algorithm>
#include <map>
#include <memory>
#include <stdexcept>
#include <vector>

#include <gtest/gtest.h>

#include "core/typeutils/LongSerializer.h"
#include "runtime/state/DefaultKeyedStateStore.h"
#include "runtime/state/VoidNamespace.h"
#include "runtime/state/heap/CopyOnWriteStateTable.h"
#include "runtime/state/HeapKeyedStateBackend.h"
#include "runtime/state/InternalKeyContextImpl.h"
#include "runtime/state/KeyGroupRange.h"
#include "streaming/api/operators/StreamingRuntimeContext.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"
#include "table/typeutils/InternalTypeInfo.h"
#include "table/types/logical/RowType.h"

namespace omnistream {
namespace {
class JoinRecordStateViewsTest : public testing::Test {
protected:
    void SetUp() override
    {
        range_ = new KeyGroupRange(0, 9);
        keyContext_ = new InternalKeyContextImpl<long>(range_, 10);
        keyContext_->setCurrentKey(1L);
        keySerializer_ = new LongSerializer();
        backend_ = new HeapKeyedStateBackend<long>(keySerializer_, keyContext_);
        stateStore_ = new DefaultKeyedStateStore<long>(backend_);
        runtimeContext_ = new StreamingRuntimeContext<long>(stateStore_, nullptr);
        recordSerializer_ = new BinaryRowDataSerializer(1);
        recordRowType_ = new RowType(false, std::vector<RowField>{RowField("value", BasicLogicalType::BIGINT)});
        recordType_ = new InternalTypeInfo(recordRowType_, recordSerializer_);
    }

    void TearDown() override
    {
        delete runtimeContext_;
        delete stateStore_;
        delete backend_;
        delete keyContext_;
        delete range_;
        delete recordType_;
        delete recordRowType_;
        // MapStateDescriptor owns recordSerializer_ and is released by backend_.
    }

    static std::shared_ptr<RowData> makeRecord(long value)
    {
        auto* row = BinaryRowData::createBinaryRowDataWithMem(1);
        row->setLong(0, value);
        return std::shared_ptr<RowData>(row);
    }

    static std::vector<long> collectRecords(JoinRecordStateView& view)
    {
        std::vector<long> values;
        auto iterator = view.getRecords();
        while (iterator->hasNext()) {
            auto record = iterator->next();
            values.push_back(*record->getLong(0));
        }
        std::sort(values.begin(), values.end());
        return values;
    }

    KeyGroupRange* range_{};
    InternalKeyContextImpl<long>* keyContext_{};
    LongSerializer* keySerializer_{};
    HeapKeyedStateBackend<long>* backend_{};
    DefaultKeyedStateStore<long>* stateStore_{};
    StreamingRuntimeContext<long>* runtimeContext_{};
    BinaryRowDataSerializer* recordSerializer_{};
    RowType* recordRowType_{};
    InternalTypeInfo* recordType_{};
};

TEST_F(JoinRecordStateViewsTest, CreatesInputSideHasNoUniqueKeyView)
{
    auto view = JoinRecordStateViews::create<long>(runtimeContext_, "test-records", recordType_);

    EXPECT_EQ(view->getJoinRecordStateViewType(), JoinRecordStateView::INPUT_SIDE_HAS_NO_UNIQUE_KEY);
    EXPECT_EQ(view->getStateType(), StateType::HEAP);
    EXPECT_FALSE(JoinRecordStateView::isOuterJoinRecordStateViewType(view->getJoinRecordStateViewType()));
    EXPECT_FALSE(view->getRecords()->hasNext());
}

TEST_F(JoinRecordStateViewsTest, AddsRetractsAndExpandsDuplicateRecords)
{
    auto view = JoinRecordStateViews::create<long>(runtimeContext_, "test-records", recordType_);

    view->addRecord(makeRecord(10));
    view->addRecord(makeRecord(10));
    view->addRecord(makeRecord(20));
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{10, 10, 20}));

    view->retractRecord(makeRecord(10));
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{10, 20}));

    view->retractRecord(makeRecord(30));
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{10, 20}));

    view->retractRecord(makeRecord(10));
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{20}));

    view->retractRecord(makeRecord(10));
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{20}));
}

TEST_F(JoinRecordStateViewsTest, IsolatesRecordsByCurrentJoinKey)
{
    auto view = JoinRecordStateViews::create<long>(runtimeContext_, "test-records", recordType_);

    keyContext_->setCurrentKey(1L);
    view->addRecord(makeRecord(10));

    keyContext_->setCurrentKey(2L);
    EXPECT_TRUE(collectRecords(*view).empty());
    view->addRecord(makeRecord(20));
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{20}));

    keyContext_->setCurrentKey(1L);
    EXPECT_EQ(collectRecords(*view), (std::vector<long>{10}));
}

// Flink InputSideHasNoUniqueKey stores MapState<RowData, Integer>.
// The user key is the record and the value is how many copies sit under the join key.
TEST_F(JoinRecordStateViewsTest, NoUniqueKeyMapMatchesFlinkCountState)
{
    auto left = JoinRecordStateViews::create<long>(runtimeContext_, "left-records", recordType_);
    left->addRecord(makeRecord(10));
    left->addRecord(makeRecord(10));
    left->addRecord(makeRecord(20));

    using CountMap = emhash7::HashMap<std::shared_ptr<RowData>, int32_t>;
    auto* table = reinterpret_cast<CopyOnWriteStateTable<long, VoidNamespace, CountMap*>*>(
        backend_->getStateTablePtr("left-records"));
    auto* entries = table->get(VoidNamespace());
    ASSERT_NE(entries, nullptr);
    EXPECT_EQ(entries->size(), 2u);

    std::map<long, int32_t> counts;
    for (const auto& entry : *entries) {
        counts[*entry.first->getLong(0)] = entry.second;
    }
    EXPECT_EQ(counts[10], 2);
    EXPECT_EQ(counts[20], 1);

    left->retractRecord(makeRecord(10));
    counts.clear();
    for (const auto& entry : *entries) {
        counts[*entry.first->getLong(0)] = entry.second;
    }
    EXPECT_EQ(counts[10], 1);
    EXPECT_EQ(counts[20], 1);
}

TEST_F(JoinRecordStateViewsTest, RightRecordsUseTheSameCountMap)
{
    auto right = JoinRecordStateViews::create<long>(runtimeContext_, "right-records", recordType_);
    right->addRecord(makeRecord(10));

    using CountMap = emhash7::HashMap<std::shared_ptr<RowData>, int32_t>;
    auto* table = reinterpret_cast<CopyOnWriteStateTable<long, VoidNamespace, CountMap*>*>(
        backend_->getStateTablePtr("right-records"));
    auto* entries = table->get(VoidNamespace());
    ASSERT_NE(entries, nullptr);
    ASSERT_EQ(entries->size(), 1u);
    EXPECT_EQ(*entries->begin()->first->getLong(0), 10);
    EXPECT_EQ(entries->begin()->second, 1);
}
} // namespace
} // namespace omnistream
