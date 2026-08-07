/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of the Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/memory/DataOutputSerializer.h"
#include "core/typeutils/ListSerializer.h"
#include "core/typeutils/LongSerializer.h"
#include "runtime/checkpoint/WindowJoinSavepointAdaptor.h"
#include "runtime/state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/restore/RestoreKVStateVB.h"
#include "table/typeutils/RowDataSerializer.h"
#include "table/types/logical/RowType.h"

using namespace omnistream;

namespace {

constexpr const char* LEFT_STATE_NAME = "left-records";
constexpr const char* RIGHT_STATE_NAME = "right-records";

std::vector<int8_t> copyOutput(DataOutputSerializer& output)
{
    return std::vector<int8_t>(
        reinterpret_cast<int8_t*>(output.getData()),
        reinterpret_cast<int8_t*>(output.getData() + output.getPosition()));
}

std::vector<int8_t> makeSerializedRow(const std::vector<uint8_t>& payload)
{
    DataOutputSerializer output;
    OutputBufferStatus outputStatus{};
    output.setBackendBuffer(&outputStatus);
    output.writeInt(static_cast<uint32_t>(payload.size()));
    if (!payload.empty()) {
        output.write(
            const_cast<uint8_t*>(payload.data()),
            static_cast<int>(payload.size()),
            0,
            static_cast<int>(payload.size()));
    }
    return copyOutput(output);
}

std::vector<int8_t> makeListValue(const std::vector<std::vector<int8_t>>& rows, char delimiter = ',')
{
    std::vector<int8_t> value;
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i != 0) {
            value.push_back(static_cast<int8_t>(delimiter));
        }
        value.insert(value.end(), rows[i].begin(), rows[i].end());
    }
    return value;
}

class RecordingRestoreKVStateVB : public RestoreKVStateVB {
public:
    ComboId appendRowToVectorBatch(const RowDataView& row) override
    {
        appendedRows.push_back(*row.valueBytes);
        appendedColumnTypes.push_back(*row.columnTypes);
        return nextComboId++;
    }

    void writeComboIdList(const std::vector<int8_t>& keyBytes, const std::vector<ComboId>& comboIds) override
    {
        writtenKeyBytes = keyBytes;
        writtenComboIds = comboIds;
    }

    int getKeyGroupPrefixBytes() const override
    {
        return 1;
    }

    void resetBatchId() override
    {
    }

    void setKeyGroupId(int) override
    {
    }

    std::vector<std::vector<int8_t>> appendedRows;
    std::vector<std::vector<omniruntime::type::DataTypeId>> appendedColumnTypes;
    std::vector<int8_t> writtenKeyBytes;
    std::vector<ComboId> writtenComboIds;
    ComboId nextComboId = 100;

protected:
    void flushVectorBatchIfNotEmpty() override
    {
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

class WindowJoinSavepointAdaptorTest : public ::testing::Test {
protected:
    void prepareAdaptor()
    {
        adaptor_.prepareForRestore({
            {"leftInputTypes", {"BIGINT", "INT"}},
            {"rightInputTypes", {"VARCHAR"}},
        });
    }

    std::shared_ptr<StateMetaInfoSnapshot> makeListMeta(
        const std::string& name,
        const std::vector<std::string>& rowTypes,
        TypeSerializer* namespaceSerializer = nullptr,
        TypeSerializer* valueSerializer = nullptr)
    {
        if (namespaceSerializer == nullptr) {
            auto ownedNamespace = std::make_unique<LongSerializer>();
            namespaceSerializer = ownedNamespace.get();
            ownedSerializers_.push_back(std::move(ownedNamespace));
        }
        if (valueSerializer == nullptr) {
            auto ownedValue =
                std::make_unique<ListSerializer>(new RowDataSerializer(new omnistream::RowType(false, rowTypes)));
            valueSerializer = ownedValue.get();
            ownedSerializers_.push_back(std::move(ownedValue));
        }

        std::unordered_map<std::string, std::string> options{{StateMetaInfoSnapshot::KEYED_STATE_TYPE, "LIST"}};
        std::unordered_map<std::string, TypeSerializer*> serializers{
            {StateMetaInfoSnapshot::COMMON_NAMESPACE_SERIALIZER_KEY, namespaceSerializer},
            {StateMetaInfoSnapshot::COMMON_VALUE_SERIALIZER_KEY, valueSerializer},
        };
        return std::make_shared<StateMetaInfoSnapshot>(
            name,
            StateMetaInfoSnapshot::BackendStateType::KEY_VALUE,
            options,
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{},
            serializers);
    }

    std::shared_ptr<StateMetaInfoSnapshot> makePriorityQueueMeta(const std::string& name = "_timer_state/window")
    {
        return std::make_shared<StateMetaInfoSnapshot>(
            name,
            StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE,
            std::unordered_map<std::string, std::string>{},
            std::unordered_map<std::string, std::shared_ptr<TypeSerializerSnapshot>>{});
    }

    WindowJoinSavepointAdaptor adaptor_;
    std::vector<std::unique_ptr<TypeSerializer>> ownedSerializers_;
};

} // namespace

TEST_F(WindowJoinSavepointAdaptorTest, PrepareForRestoreParsesBothInputSchemas)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    auto rightMeta = makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"});

    adaptor_.buildOmniMainMetaInfo(3, *leftMeta);
    adaptor_.buildOmniMainMetaInfo(7, *rightMeta);

    EXPECT_EQ(
        adaptor_.columnTypes(3),
        (std::vector<omniruntime::type::DataTypeId>{
            omniruntime::type::DataTypeId::OMNI_LONG, omniruntime::type::DataTypeId::OMNI_INT}));
    EXPECT_EQ(
        adaptor_.columnTypes(7),
        (std::vector<omniruntime::type::DataTypeId>{omniruntime::type::DataTypeId::OMNI_VARCHAR}));
    EXPECT_GT(adaptor_.batchSize(3), 0);
    EXPECT_EQ(adaptor_.batchSize(3), adaptor_.batchSize(7));
}

TEST_F(WindowJoinSavepointAdaptorTest, PrepareForRestoreRejectsMissingInputSchema)
{
    EXPECT_THROW(adaptor_.prepareForRestore({{"leftInputTypes", {"BIGINT"}}}), std::runtime_error);
    EXPECT_THROW(adaptor_.prepareForRestore({{"rightInputTypes", {"BIGINT"}}}), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, ValidateForRestoreAcceptsWindowListStatesAndTimers)
{
    prepareAdaptor();
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> metas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
        makePriorityQueueMeta(),
    };

    EXPECT_NO_THROW(adaptor_.validateForRestore(metas));
}

TEST_F(WindowJoinSavepointAdaptorTest, ValidateForRestoreRejectsInvalidSerializers)
{
    prepareAdaptor();
    auto wrongNamespace = std::make_unique<IntSerializer>();
    auto* wrongNamespacePtr = wrongNamespace.get();
    ownedSerializers_.push_back(std::move(wrongNamespace));
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> wrongNamespaceMetas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}, wrongNamespacePtr),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(wrongNamespaceMetas), std::runtime_error);

    auto wrongValue = std::make_unique<LongSerializer>();
    auto* wrongValuePtr = wrongValue.get();
    ownedSerializers_.push_back(std::move(wrongValue));
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> wrongValueMetas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}, nullptr, wrongValuePtr),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(wrongValueMetas), std::runtime_error);

    auto wrongElement = std::make_unique<ListSerializer>(new LongSerializer());
    auto* wrongElementPtr = wrongElement.get();
    ownedSerializers_.push_back(std::move(wrongElement));
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> wrongElementMetas{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}, nullptr, wrongElementPtr),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(wrongElementMetas), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, ValidateForRestoreRejectsMismatchedRowArityAndUnexpectedState)
{
    prepareAdaptor();
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> arityMismatch{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT"}),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(arityMismatch), std::runtime_error);

    std::vector<std::shared_ptr<StateMetaInfoSnapshot>> unexpectedState{
        makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"}),
        makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"}),
        makeListMeta("unexpected", {"BIGINT"}),
    };
    EXPECT_THROW(adaptor_.validateForRestore(unexpectedState), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, GetStateTypeClassifiesWindowStatesAndTimers)
{
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    auto rightMeta = makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"});
    auto otherMeta = makeListMeta("other", {"BIGINT"});
    auto timerMeta = makePriorityQueueMeta();

    EXPECT_EQ(adaptor_.getStateType(*leftMeta), RestoreStateType::KV_WITH_VB);
    EXPECT_EQ(adaptor_.getStateType(*rightMeta), RestoreStateType::KV_WITH_VB);
    EXPECT_EQ(adaptor_.getStateType(*timerMeta), RestoreStateType::PQ);
    EXPECT_EQ(adaptor_.getStateType(*otherMeta), RestoreStateType::UNSUPPORT);
}

TEST_F(WindowJoinSavepointAdaptorTest, BuildOmniMainMetaInfoMapsStateIdAndUsesComboIdListSerializer)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});

    auto omniMeta = adaptor_.buildOmniMainMetaInfo(5, *leftMeta);

    EXPECT_EQ(omniMeta.getName(), LEFT_STATE_NAME);
    EXPECT_EQ(omniMeta.getOption(StateMetaInfoSnapshot::KEYED_STATE_TYPE), "2");
    auto* listSerializer =
        dynamic_cast<ListSerializer*>(omniMeta.getTypeSerializer(StateMetaInfoSnapshot::COMMON_VALUE_SERIALIZER_KEY));
    ASSERT_NE(listSerializer, nullptr);
    ASSERT_NE(listSerializer->getElementSerializer(), nullptr);
    EXPECT_EQ(listSerializer->getElementSerializer()->getBackendId(), BackendDataType::BIGINT_BK);
    EXPECT_EQ(adaptor_.columnTypes(5).size(), 2U);
}

TEST_F(WindowJoinSavepointAdaptorTest, BuildOmniMainMetaInfoRejectsUnexpectedState)
{
    prepareAdaptor();
    auto unexpectedMeta = makeListMeta("unexpected", {"BIGINT"});
    EXPECT_THROW(adaptor_.buildOmniMainMetaInfo(1, *unexpectedMeta), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, RetrieveKVRowDataRestoresEveryListElementAndWritesComboIds)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto firstRow = makeSerializedRow({1, 2, 3, 4});
    const auto secondRow = makeSerializedRow({5, 6});
    const auto listValue = makeListValue({firstRow, secondRow});
    const std::vector<int8_t> keyBytes{9, 8, 7};
    RecordingRestoreKVStateVB writer;

    adaptor_.retrieveKVRowData(keyBytes, listValue, 4, &writer);

    EXPECT_EQ(writer.appendedRows, (std::vector<std::vector<int8_t>>{firstRow, secondRow}));
    ASSERT_EQ(writer.appendedColumnTypes.size(), 2U);
    EXPECT_EQ(writer.appendedColumnTypes[0], adaptor_.columnTypes(4));
    EXPECT_EQ(writer.appendedColumnTypes[1], adaptor_.columnTypes(4));
    EXPECT_EQ(writer.writtenKeyBytes, keyBytes);
    EXPECT_EQ(writer.writtenComboIds, (std::vector<ComboId>{100, 101}));
}

TEST_F(WindowJoinSavepointAdaptorTest, RetrieveKVRowDataRejectsInvalidArgumentsAndMalformedList)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);
    RecordingRestoreKVStateVB writer;
    const std::vector<int8_t> keyBytes{1};
    const auto row = makeSerializedRow({1, 2});

    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, row, 4, nullptr), std::runtime_error);
    EXPECT_THROW(adaptor_.retrieveKVRowData({}, row, 4, &writer), std::runtime_error);
    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, {0, 0, 0}, 4, &writer), std::runtime_error);
    EXPECT_THROW(adaptor_.retrieveKVRowData(keyBytes, makeListValue({row, row}, ';'), 4, &writer), std::runtime_error);
    EXPECT_THROW(adaptor_.columnTypes(99), std::runtime_error);
}

// ===== Tests for deserializeRows (Flink ListDelimitedSerializer format) =====

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_SingleRow)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto row = makeSerializedRow({10, 20, 30});
    // Flink ListDelimitedSerializer format for single row = just the row bytes
    // (no comma needed for single element)
    const auto listValue = row;

    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, listValue, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 1U);
    EXPECT_EQ(writer.appendedRows[0], row);
    EXPECT_EQ(writer.writtenComboIds.size(), 1U);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_MultipleRowsWithCommas)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto row1 = makeSerializedRow({1, 2, 3, 4});
    const auto row2 = makeSerializedRow({5, 6});
    const auto row3 = makeSerializedRow({7, 8, 9});
    const auto listValue = makeListValue({row1, row2, row3});

    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, listValue, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 3U);
    EXPECT_EQ(writer.appendedRows[0], row1);
    EXPECT_EQ(writer.appendedRows[1], row2);
    EXPECT_EQ(writer.appendedRows[2], row3);
    EXPECT_EQ(writer.writtenComboIds.size(), 3U);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsZeroLengthRow)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Row with zero-length payload (length=0) is rejected
    auto emptyRow = makeSerializedRow({});
    ASSERT_EQ(emptyRow.size(), 4U);  // Just the int32 length = 0

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, emptyRow, 4, &writer), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsMissingDelimiter)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    const auto row1 = makeSerializedRow({1, 2});
    const auto row2 = makeSerializedRow({3, 4});
    // Concatenate without comma between them
    std::vector<int8_t> badValue;
    badValue.insert(badValue.end(), row1.begin(), row1.end());
    badValue.insert(badValue.end(), row2.begin(), row2.end());

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, badValue, 4, &writer), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsTruncatedRow)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Write a valid row followed by a truncated row (only length prefix, no data)
    const auto row1 = makeSerializedRow({1, 2});
    std::vector<int8_t> truncatedValue;
    truncatedValue.insert(truncatedValue.end(), row1.begin(), row1.end());
    truncatedValue.push_back(static_cast<int8_t>(','));
    // Write length 100 but don't include the 100 bytes of data
    truncatedValue.push_back(static_cast<int8_t>(0));
    truncatedValue.push_back(static_cast<int8_t>(0));
    truncatedValue.push_back(static_cast<int8_t>(0));
    truncatedValue.push_back(static_cast<int8_t>(100));  // 100 bytes needed but not present

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, truncatedValue, 4, &writer), std::runtime_error);
}

TEST_F(WindowJoinSavepointAdaptorTest, DeserializeRows_RejectsNegativeRowLength)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Write a negative row length (0xFFFFFFFF = -1 in signed int32)
    std::vector<int8_t> badValue;
    badValue.push_back(static_cast<int8_t>(0xFF));
    badValue.push_back(static_cast<int8_t>(0xFF));
    badValue.push_back(static_cast<int8_t>(0xFF));
    badValue.push_back(static_cast<int8_t>(0xFF));

    RecordingRestoreKVStateVB writer;
    EXPECT_THROW(adaptor_.retrieveKVRowData({1}, badValue, 4, &writer), std::runtime_error);
}

// ===== Tests for Flink serialization format round-trip =====

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_SingleRowRoundTrip)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Simulate what serializeFlinkRowDataList would produce for a single row
    const auto row = makeSerializedRow({42, 99});

    // The Flink ListDelimitedSerializer format for 1 element = [element bytes]
    // (no delimiter needed)
    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, row, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 1U);
    EXPECT_EQ(writer.appendedRows[0], row);
}

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_MultipleRowsRoundTrip)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    adaptor_.buildOmniMainMetaInfo(4, *leftMeta);

    // Simulate what serializeFlinkRowDataList produces for multiple rows:
    // [row1Bytes][','][row2Bytes][','][row3Bytes]
    const auto row1 = makeSerializedRow({10, 20});
    const auto row2 = makeSerializedRow({30, 40});
    const auto row3 = makeSerializedRow({50, 60});

    auto flinkValue = makeListValue({row1, row2, row3});

    RecordingRestoreKVStateVB writer;
    adaptor_.retrieveKVRowData({1}, flinkValue, 4, &writer);

    ASSERT_EQ(writer.appendedRows.size(), 3U);
    EXPECT_EQ(writer.appendedRows[0], row1);
    EXPECT_EQ(writer.appendedRows[1], row2);
    EXPECT_EQ(writer.appendedRows[2], row3);
    EXPECT_EQ(writer.writtenComboIds.size(), 3U);
}

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_CommaBetweenRowsNotBeforeFirst)
{
    // Verify that the Flink format has NO comma before the first element
    // and NO comma after the last element
    const auto row1 = makeSerializedRow({1});
    const auto row2 = makeSerializedRow({2});

    // Format: [row1][','][row2]
    auto valueWithCommaBetween = makeListValue({row1, row2});

    // Manually verify the format
    size_t firstCommaPos = 0;
    bool foundComma = false;
    for (size_t i = 0; i < valueWithCommaBetween.size(); ++i) {
        if (valueWithCommaBetween[i] == static_cast<int8_t>(',')) {
            firstCommaPos = i;
            foundComma = true;
            break;
        }
    }
    ASSERT_TRUE(foundComma);
    // Comma must be exactly at the end of the first row (not before, not inside)
    EXPECT_EQ(firstCommaPos, row1.size());
    // Comma must NOT be at the very start
    EXPECT_GT(firstCommaPos, 0U);
    // Comma must NOT be at the very end (no trailing delimiter)
    EXPECT_LT(firstCommaPos, valueWithCommaBetween.size() - 1);
}

TEST_F(WindowJoinSavepointAdaptorTest, FlinkSerializationFormat_LeftAndRightStatesSeparately)
{
    prepareAdaptor();
    auto leftMeta = makeListMeta(LEFT_STATE_NAME, {"BIGINT", "INT"});
    auto rightMeta = makeListMeta(RIGHT_STATE_NAME, {"VARCHAR"});
    adaptor_.buildOmniMainMetaInfo(1, *leftMeta);
    adaptor_.buildOmniMainMetaInfo(2, *rightMeta);

    const auto leftRow1 = makeSerializedRow({100, 200});
    const auto leftRow2 = makeSerializedRow({300, 400});
    auto leftValue = makeListValue({leftRow1, leftRow2});

    const auto rightRow = makeSerializedRow({1, 2, 3});
    auto rightValue = rightRow;  // single row, no comma needed

    RecordingRestoreKVStateVB leftWriter;
    adaptor_.retrieveKVRowData({1}, leftValue, 1, &leftWriter);
    ASSERT_EQ(leftWriter.appendedRows.size(), 2U);
    EXPECT_EQ(leftWriter.appendedRows[0], leftRow1);
    EXPECT_EQ(leftWriter.appendedRows[1], leftRow2);

    RecordingRestoreKVStateVB rightWriter;
    adaptor_.retrieveKVRowData({2}, rightValue, 2, &rightWriter);
    ASSERT_EQ(rightWriter.appendedRows.size(), 1U);
    EXPECT_EQ(rightWriter.appendedRows[0], rightRow);
}
