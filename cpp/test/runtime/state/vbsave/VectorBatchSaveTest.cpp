#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/memory/DataInputDeserializer.h"
#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"

namespace {

class MockRowData : public RowData {
public:
    MockRowData() : RowData(RowData::GenericRowDataID)
    {
    }

    int getArity() override
    {
        return 0;
    }

    void setRowKind(RowKind kind) override
    {
        kind_ = kind;
    }

    bool isNullAt(int /*pos*/) override
    {
        return true;
    }

    long* getLong(int /*pos*/) override
    {
        return nullptr;
    }

    bool* getBool(int /*pos*/) override
    {
        return nullptr;
    }

    int* getInt(int /*pos*/) override
    {
        return nullptr;
    }

    RowKind getRowKind() override
    {
        return kind_;
    }

    TimestampData getTimestamp(int /*pos*/) override
    {
        return TimestampData(0, 0);
    }

    TimestampData getTimestampPrecise(int /*pos*/) override
    {
        return TimestampData(0, 0);
    }

    bool operator==(const RowData& other) const override
    {
        return this == &other;
    }

    int hashCode() const override
    {
        return 0;
    }

    int hashCodeFast() const override
    {
        return 0;
    }

private:
    RowKind kind_ = RowKind::INSERT;
};

class MockSerializer : public TypeSerializer {};

// 录制 serializer：serialize 时写入固定 4 字节，用于验证 serializeRowData 的成功路径。
class RecordingSerializer : public TypeSerializer {
public:
    void serialize(void* /*row*/, DataOutputSerializer& target) override
    {
        target.writeInt(0xDEADBEEF);
    }

    void* deserialize(DataInputView& /*source*/) override
    {
        return nullptr;
    }
};

class MockVectorBatchStateAccessor : public VectorBatchStateAccessor {
public:
    bool getSerializedBatch(omnistream::VectorBatchId /*batchId*/, ByteView* /*value*/) override
    {
        return false;
    }

    void close() override
    {
    }
};

class MockHooks : public omnistream::VectorBatchSaveHooks {
public:
    std::vector<omnistream::VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& /*snapshotResources*/, const omnistream::VectorBatchSavePlan& /*plan*/) override
    {
        return {};
    }

    omnistream::ComboId parseVectorBatchReference(
        ByteView value,
        const omnistream::VectorBatchSaveStateContext& /*context*/,
        const omnistream::VectorBatchSavePlan& /*plan*/) override
    {
        return omnistream::VectorBatchSaveTools::parseComboId(value);
    }

    std::vector<int8_t> encodeFlinkLogicalValue(
        const KeyValueStateIterator::CurrentEntry& entry,
        RowData& /*row*/,
        const omnistream::VectorBatchSaveStateContext& /*context*/,
        const omnistream::VectorBatchSavePlan& /*plan*/) override
    {
        return std::vector<int8_t>(entry.value.begin(), entry.value.end());
    }
};

std::vector<int8_t> bytes(std::initializer_list<int8_t> values)
{
    return std::vector<int8_t>(values);
}

void writeInt32BE(std::vector<uint8_t>& buf, int32_t val)
{
    buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
}

void writeComboIdBE(std::vector<uint8_t>& buf, uint64_t id)
{
    for (int i = 56; i >= 0; i -= 8) {
        buf.push_back(static_cast<uint8_t>((id >> i) & 0xFF));
    }
}

// Heap ListState format: [int32 list_size][comboId_1 (8B)][comboId_2 (8B)]...
std::vector<uint8_t> makeHeapListValue(const std::vector<uint64_t>& comboIds)
{
    std::vector<uint8_t> result;
    writeInt32BE(result, static_cast<int32_t>(comboIds.size()));
    for (auto id : comboIds) {
        writeComboIdBE(result, id);
    }
    return result;
}

// RocksDB ListState format: [comboId_1 (8B)][','][comboId_2 (8B)][',']...
std::vector<uint8_t> makeRocksDbListValue(const std::vector<uint64_t>& comboIds)
{
    std::vector<uint8_t> result;
    for (size_t i = 0; i < comboIds.size(); ++i) {
        if (i > 0) {
            result.push_back(static_cast<uint8_t>(','));
        }
        writeComboIdBE(result, comboIds[i]);
    }
    return result;
}

} // namespace

TEST(VectorBatchSaveTest, DetectsOnlyNamesEndingWithVb)
{
    EXPECT_TRUE(omnistream::VectorBatchSaveTools::isVbStateName("statevb"));
    EXPECT_TRUE(omnistream::VectorBatchSaveTools::isVbStateName("vb"));

    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName(""));
    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName("v"));
    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName("vbState"));
    EXPECT_FALSE(omnistream::VectorBatchSaveTools::isVbStateName("stateVB"));
}

TEST(VectorBatchSaveTest, ParsesBigEndianComboId)
{
    const std::vector<uint8_t> positive = {0x01, 0x23, 0x45, 0x67, 0x7F, 0x00, 0x10, 0x20};
    EXPECT_EQ(
        omnistream::VectorBatchSaveTools::parseComboId(ByteView(positive.data(), positive.size())),
        0x012345677F001020LL);
}

TEST(VectorBatchSaveTest, ParsesZeroAndSmallComboIds)
{
    const std::vector<uint8_t> zero = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    const std::vector<uint8_t> small = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02};

    EXPECT_EQ(omnistream::VectorBatchSaveTools::parseComboId(ByteView(zero.data(), zero.size())), 0);
    EXPECT_EQ(omnistream::VectorBatchSaveTools::parseComboId(ByteView(small.data(), small.size())), 258);
}

TEST(VectorBatchSaveTest, ParseComboIdRejectsShortInput)
{
    const std::vector<uint8_t> shortValue = {0x00, 0x00, 0x01};

    try {
        omnistream::VectorBatchSaveTools::parseComboId(ByteView(shortValue.data(), shortValue.size()));
        FAIL() << "Expected runtime_error";
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        EXPECT_NE(message.find("at least 8 bytes"), std::string::npos);
        EXPECT_NE(message.find("actual size=3"), std::string::npos);
    }
}

TEST(VectorBatchSaveTest, ParseComboIdRejectsEmptyInput)
{
    try {
        omnistream::VectorBatchSaveTools::parseComboId(ByteView());
        FAIL() << "Expected runtime_error";
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        EXPECT_NE(message.find("at least 8 bytes"), std::string::npos);
        EXPECT_NE(message.find("actual size=0"), std::string::npos);
    }
}

TEST(VectorBatchSaveTest, SerializeRowDataReturnsEmptyForNullInputs)
{
    MockSerializer serializer;
    MockRowData row;

    EXPECT_TRUE(omnistream::VectorBatchSaveTools::serializeRowData(nullptr, &serializer).empty());
    EXPECT_TRUE(omnistream::VectorBatchSaveTools::serializeRowData(&row, nullptr).empty());
}

TEST(VectorBatchSaveTest, SkipVbValueTagReturnsPayloadView)
{
    const auto tagged = bytes({0x7F, 0x01, 0x02, 0x03});

    ByteView payload = omnistream::VectorBatchSaveTools::skipVbValueTag(ByteView(tagged.data(), tagged.size()));

    ASSERT_EQ(payload.size(), 3);
    EXPECT_EQ(payload[0], 0x01);
    EXPECT_EQ(payload[1], 0x02);
    EXPECT_EQ(payload[2], 0x03);
    EXPECT_EQ(payload.data(), reinterpret_cast<const uint8_t*>(tagged.data()) + 1);
}

TEST(VectorBatchSaveTest, SkipVbValueTagReturnsEmptyForMissingPayload)
{
    const auto empty = bytes({});
    const auto tagOnly = bytes({0x7F});

    EXPECT_TRUE(omnistream::VectorBatchSaveTools::skipVbValueTag(ByteView(empty.data(), empty.size())).empty());
    EXPECT_TRUE(omnistream::VectorBatchSaveTools::skipVbValueTag(ByteView(tagOnly.data(), tagOnly.size())).empty());
}

TEST(VectorBatchSaveTest, SaveStateContextRequiresAccessorOnlyForVectorBatchStates)
{
    MockSerializer serializer;
    auto accessor = std::make_shared<MockVectorBatchStateAccessor>();

    omnistream::VectorBatchSaveStateContext context;
    EXPECT_FALSE(context.isValid());

    context.writable = true;
    EXPECT_FALSE(context.isValid());

    context.valueSerializer = &serializer;
    EXPECT_FALSE(context.isValid());

    context.mappedKvStateId = 0;
    EXPECT_TRUE(context.isValid());

    context.stateType = omnistream::VectorBatchStateType::PQ;
    EXPECT_TRUE(context.isValid());

    context.stateType = omnistream::VectorBatchStateType::KV_WITH_VB;
    EXPECT_FALSE(context.isValid());

    context.vbAccessor = accessor;
    EXPECT_TRUE(context.isValid());

    context.writable = false;
    EXPECT_FALSE(context.isValid());
}

TEST(VectorBatchSaveTest, PlanDefaultsAreEmptyAndUnset)
{
    omnistream::VectorBatchSavePlan plan;

    EXPECT_TRUE(plan.targetMetaInfos.empty());
    EXPECT_TRUE(plan.mainStateIds.empty());
    EXPECT_TRUE(plan.kvStateIdMapping.empty());
    EXPECT_TRUE(plan.stateContextSpecs.empty());
    EXPECT_EQ(plan.keyGroupRange, nullptr);
    EXPECT_TRUE(plan.keySerializerJson.empty());
}

TEST(VectorBatchSaveTest, StateContextSpecCarriesSourceStateAndAccessorOptions)
{
    MockSerializer serializer;
    omnistream::VectorBatchSavePlan::StateContextSpec spec;
    spec.sourceKvStateId = 7;
    spec.logicalStateName = "orders";
    spec.valueSerializer = &serializer;
    spec.accessorOptions.maxDecodedBatchCacheBytes = 4096;

    EXPECT_EQ(spec.sourceKvStateId, 7);
    EXPECT_EQ(spec.logicalStateName, "orders");
    EXPECT_EQ(spec.valueSerializer, &serializer);
    EXPECT_EQ(spec.accessorOptions.maxDecodedBatchCacheBytes, 4096);
}

TEST(VectorBatchSaveTest, DefaultEncodeFlinkLogicalKeyKeepsOriginalKeyBytes)
{
    const auto key = bytes({0x10, 0x20, 0x30});
    const auto value = bytes({0x01});
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(key.data(), key.size());
    entry.value = ByteView(value.data(), value.size());

    MockHooks hooks;
    MockRowData row;
    omnistream::VectorBatchSaveStateContext context;
    omnistream::VectorBatchSavePlan plan;

    EXPECT_EQ(hooks.encodeFlinkLogicalKey(entry, row, context, plan), key);
}

// ===== parseComboIdList tests =====

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_SingleElement)
{
    const std::vector<uint64_t> comboIds = {0x0123456789ABCDEFULL};
    auto value = makeHeapListValue(comboIds);
    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), true);

    ASSERT_EQ(result.size(), 1U);
    EXPECT_EQ(result[0], 0x0123456789ABCDEFULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_MultipleElements)
{
    const std::vector<uint64_t> comboIds = {1ULL, 0xFFFFFFFFULL, 0x1234567890ABCDEFULL};
    auto value = makeHeapListValue(comboIds);
    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), true);

    ASSERT_EQ(result.size(), 3U);
    EXPECT_EQ(result[0], 1ULL);
    EXPECT_EQ(result[1], 0xFFFFFFFFULL);
    EXPECT_EQ(result[2], 0x1234567890ABCDEFULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_ZeroElementList)
{
    // Heap format with 0 elements = [int32 size=0] = 4 bytes.
    const std::vector<uint64_t> emptyList;
    auto value = makeHeapListValue(emptyList);
    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), true);

    EXPECT_TRUE(result.empty());
}

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_EdgeCase_ZeroComboId)
{
    const std::vector<uint64_t> comboIds = {0ULL, 0ULL, 0ULL};
    auto value = makeHeapListValue(comboIds);
    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), true);

    ASSERT_EQ(result.size(), 3U);
    for (auto id : result) {
        EXPECT_EQ(id, 0ULL);
    }
}

TEST(VectorBatchSaveTest, ParseComboIdList_RocksDbFormat_SingleElement)
{
    const std::vector<uint64_t> comboIds = {0x0123456789ABCDEFULL};
    auto value = makeRocksDbListValue(comboIds);
    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), false);

    ASSERT_EQ(result.size(), 1U);
    EXPECT_EQ(result[0], 0x0123456789ABCDEFULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RocksDbFormat_MultipleElements)
{
    const std::vector<uint64_t> comboIds = {100ULL, 200ULL, 300ULL, 400ULL};
    auto value = makeRocksDbListValue(comboIds);
    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), false);

    ASSERT_EQ(result.size(), 4U);
    EXPECT_EQ(result[0], 100ULL);
    EXPECT_EQ(result[1], 200ULL);
    EXPECT_EQ(result[2], 300ULL);
    EXPECT_EQ(result[3], 400ULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RocksDbFormat_CommaDelimitersCorrect)
{
    std::vector<uint8_t> value;
    writeComboIdBE(value, 42ULL);
    value.push_back(static_cast<uint8_t>(','));
    writeComboIdBE(value, 99ULL);
    value.push_back(static_cast<uint8_t>(','));
    writeComboIdBE(value, 7ULL);

    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), false);

    ASSERT_EQ(result.size(), 3U);
    EXPECT_EQ(result[0], 42ULL);
    EXPECT_EQ(result[1], 99ULL);
    EXPECT_EQ(result[2], 7ULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RocksDbFormat_NoTrailingDelimiter)
{
    std::vector<uint8_t> value;
    writeComboIdBE(value, 111ULL);
    value.push_back(static_cast<uint8_t>(','));
    writeComboIdBE(value, 222ULL);

    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), false);

    ASSERT_EQ(result.size(), 2U);
    EXPECT_EQ(result[0], 111ULL);
    EXPECT_EQ(result[1], 222ULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RejectsShortInput)
{
    const std::vector<uint8_t> shortValue = {0x00, 0x01, 0x02};
    EXPECT_THROW(
        omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(shortValue.data(), shortValue.size()), false),
        std::runtime_error);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RejectsEmptyData)
{
    EXPECT_THROW(omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(), false), std::runtime_error);
}

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_ExplicitBackend)
{
    // Heap format: [int32 size=1][8 bytes comboId] = 12 bytes total
    std::vector<uint8_t> heapValue;
    writeInt32BE(heapValue, 1);
    writeComboIdBE(heapValue, 12345ULL);

    auto result =
        omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(heapValue.data(), heapValue.size()), true);
    ASSERT_EQ(result.size(), 1U);
    EXPECT_EQ(result[0], 12345ULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RocksDbFormat_ExplicitBackend)
{
    // Two comboIds in RocksDB format (with comma delimiter).
    std::vector<uint8_t> value;
    writeComboIdBE(value, 1000ULL);
    value.push_back(static_cast<uint8_t>(','));
    writeComboIdBE(value, 2000ULL);

    auto result = omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), false);
    ASSERT_EQ(result.size(), 2U);
    EXPECT_EQ(result[0], 1000ULL);
    EXPECT_EQ(result[1], 2000ULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_MultipleElementsExplicit)
{
    // Heap with 2 elements (20 bytes)
    std::vector<uint8_t> heapValue;
    writeInt32BE(heapValue, 2);
    writeComboIdBE(heapValue, 10ULL);
    writeComboIdBE(heapValue, 20ULL);
    EXPECT_EQ(heapValue.size(), 20U);

    auto result =
        omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(heapValue.data(), heapValue.size()), true);
    ASSERT_EQ(result.size(), 2U);
    EXPECT_EQ(result[0], 10ULL);
    EXPECT_EQ(result[1], 20ULL);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RocksDbFormat_RejectsInvalidDelimiter)
{
    // RocksDB format with ';' instead of ',' as delimiter — should reject
    std::vector<uint8_t> value;
    writeComboIdBE(value, 100ULL);
    value.push_back(static_cast<uint8_t>(';')); // invalid delimiter
    writeComboIdBE(value, 200ULL);

    EXPECT_THROW(
        omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), false),
        std::runtime_error);
}

TEST(VectorBatchSaveTest, ParseComboIdList_RocksDbFormat_RejectsTrailingBytes)
{
    // RocksDB format with extra trailing byte after last comboId
    std::vector<uint8_t> value;
    writeComboIdBE(value, 100ULL);
    value.push_back(static_cast<uint8_t>(','));
    writeComboIdBE(value, 200ULL);
    value.push_back(static_cast<uint8_t>(0xFF)); // trailing garbage

    EXPECT_THROW(
        omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), false),
        std::runtime_error);
}

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_RejectsNegativeSize)
{
    // Heap format with negative list size: [int32 -1] = 4 bytes
    std::vector<uint8_t> value;
    writeInt32BE(value, -1);

    EXPECT_THROW(
        omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), true),
        std::runtime_error);
}

TEST(VectorBatchSaveTest, ParseComboIdList_HeapFormat_RejectsNegativeSizeWithExceptionMessage)
{
    // Heap format with negative list size: verify error message contains "invalid heap list size"
    std::vector<uint8_t> value;
    writeInt32BE(value, -3);

    try {
        omnistream::VectorBatchSaveTools::parseComboIdList(ByteView(value.data(), value.size()), true);
        FAIL() << "Expected runtime_error";
    } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        EXPECT_NE(message.find("invalid heap list size"), std::string::npos) << "Message: " << message;
    }
}

// ============================================================================
// VectorBatchSaveTools::serializeRowData 成功路径
// ============================================================================

TEST(VectorBatchSaveTest, SerializeRowDataReturnsBytesForValidInput)
{
    RecordingSerializer serializer;
    MockRowData row;

    auto result = omnistream::VectorBatchSaveTools::serializeRowData(&row, &serializer);

    // RecordingSerializer writes 4 bytes (int32 0xDEADBEEF)
    ASSERT_EQ(result.size(), 4U);
    EXPECT_EQ(result[0], static_cast<int8_t>(0xDE));
    EXPECT_EQ(result[1], static_cast<int8_t>(0xAD));
    EXPECT_EQ(result[2], static_cast<int8_t>(0xBE));
    EXPECT_EQ(result[3], static_cast<int8_t>(0xEF));
}

// ============================================================================
// VectorBatchSaveHooks 默认实现测试
// ============================================================================

TEST(VectorBatchSaveTest, DefaultParseVectorBatchReferenceReturnsMinusOne)
{
    // 使用基类指针调用默认实现，验证返回 -1
    std::unique_ptr<omnistream::VectorBatchSaveHooks> hooks = std::make_unique<MockHooks>();

    // 直接调用基类默认实现（通过 MockHooks 的 override 实际上调用的是 MockHooks 的版本，
    // 用另一个只继承不覆盖的类来测试真正的默认实现）
    class DefaultHooks : public omnistream::VectorBatchSaveHooks {
    public:
        std::vector<omnistream::VectorBatchSaveStateContext> buildSaveStateContexts(
            FullSnapshotResources&, const omnistream::VectorBatchSavePlan&) override
        {
            return {};
        }
    };

    DefaultHooks defaultHooks;
    ByteView empty;
    omnistream::VectorBatchSaveStateContext context;
    omnistream::VectorBatchSavePlan plan;

    EXPECT_EQ(defaultHooks.parseVectorBatchReference(empty, context, plan), static_cast<omnistream::ComboId>(-1));
}

TEST(VectorBatchSaveTest, DefaultEncodeFlinkLogicalValueReturnsEmpty)
{
    class DefaultHooks : public omnistream::VectorBatchSaveHooks {
    public:
        std::vector<omnistream::VectorBatchSaveStateContext> buildSaveStateContexts(
            FullSnapshotResources&, const omnistream::VectorBatchSavePlan&) override
        {
            return {};
        }
    };

    DefaultHooks defaultHooks;
    const auto key = bytes({0x10, 0x20});
    const auto value = bytes({0x01});
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(key.data(), key.size());
    entry.value = ByteView(value.data(), value.size());
    MockRowData row;
    omnistream::VectorBatchSaveStateContext context;
    omnistream::VectorBatchSavePlan plan;

    EXPECT_TRUE(defaultHooks.encodeFlinkLogicalValue(entry, row, context, plan).empty());
}
