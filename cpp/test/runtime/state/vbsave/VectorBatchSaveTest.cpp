#include <gtest/gtest.h>

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "runtime/state/vbsave/VectorBatchSaveHooks.h"
#include "runtime/state/vbsave/VectorBatchSavePlan.h"
#include "runtime/state/vbsave/VectorBatchSaveTools.h"
#include "core/memory/DataInputDeserializer.h"
#include "table/data/binary/BinaryRowData.h"
#include "table/typeutils/BinaryRowDataSerializer.h"

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

class MockVectorBatchStateAccessor : public VectorBatchStateAccessor {
public:
    bool getSerializedBatch(omnistream::VectorBatchId /*batchId*/, ByteView* /*value*/) override
    {
        return false;
    }

    void close() override
    {
        ++closeCalls;
    }

    int closeCalls = 0;
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

// 不覆盖 encodeFlinkLogicalValue / parseVectorBatchReference 的最小模拟类，
// 用于测试 VectorBatchSaveHooks 的默认实现行为。
class MinimalHooks : public omnistream::VectorBatchSaveHooks {
public:
    std::vector<omnistream::VectorBatchSaveStateContext> buildSaveStateContexts(
        FullSnapshotResources& /*snapshotResources*/, const omnistream::VectorBatchSavePlan& /*plan*/) override
    {
        return {};
    }
};

std::vector<int8_t> bytes(std::initializer_list<int8_t> values)
{
    return std::vector<int8_t>(values);
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

// ============================================================================
// VectorBatchSaveTools — serializeRowData 快乐路径
// ============================================================================

TEST(VectorBatchSaveTest, SerializeRowDataWithBinaryRowData)
{
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(1);
    row->setLong(0, 42);

    BinaryRowDataSerializer serializer(1);
    auto result = omnistream::VectorBatchSaveTools::serializeRowData(row, &serializer);

    // BinaryRowDataSerializer 写入格式：int32 (size) + 二进制数据，
    // 因此结果至少包含 4 字节以上的 payload。
    ASSERT_FALSE(result.empty());
    EXPECT_GE(result.size(), static_cast<size_t>(4));

    // 反序列化回来验证内容正确
    DataInputDeserializer input(reinterpret_cast<const uint8_t*>(result.data()), static_cast<int>(result.size()), 0);
    int size = input.readInt();
    EXPECT_GT(size, 0);
    EXPECT_EQ(size + static_cast<int>(sizeof(int32_t)), static_cast<int>(result.size()));

    delete row;
}

TEST(VectorBatchSaveTest, SerializeRowDataWithMultiFieldRow)
{
    BinaryRowData* row = BinaryRowData::createBinaryRowDataWithMem(3);
    row->setLong(0, 100);
    row->setLong(1, 200);
    row->setLong(2, 300);

    BinaryRowDataSerializer serializer(3);
    auto result = omnistream::VectorBatchSaveTools::serializeRowData(row, &serializer);

    ASSERT_FALSE(result.empty());

    // 反序列化验证
    DataInputDeserializer input(reinterpret_cast<const uint8_t*>(result.data()), static_cast<int>(result.size()), 0);
    int size = input.readInt();
    EXPECT_GT(size, 0);

    delete row;
}

// ============================================================================
// VectorBatchSavePlan — VectorBatchSaveStateContext move 语义
// ============================================================================

TEST(VectorBatchSaveTest, SaveStateContextMoveConstructorResetsSource)
{
    auto accessor = std::make_shared<MockVectorBatchStateAccessor>();
    MockSerializer serializer;

    omnistream::VectorBatchSaveStateContext ctx;
    ctx.writable = true;
    ctx.mappedKvStateId = 5;
    ctx.logicalStateName = "testState";
    ctx.valueSerializer = &serializer;
    ctx.vbAccessor = accessor;
    ctx.stateType = omnistream::VectorBatchStateType::KV_WITH_VB;

    omnistream::VectorBatchSaveStateContext moved(std::move(ctx));

    // 目标对象应正确获取所有字段
    EXPECT_TRUE(moved.isValid());
    EXPECT_EQ(moved.mappedKvStateId, 5);
    EXPECT_EQ(moved.logicalStateName, "testState");
    EXPECT_EQ(moved.valueSerializer, &serializer);
    EXPECT_EQ(moved.vbAccessor.get(), accessor.get());
    EXPECT_EQ(moved.stateType, omnistream::VectorBatchStateType::KV_WITH_VB);

    // 源对象应被重置为默认值
    EXPECT_FALSE(ctx.isValid());
    EXPECT_EQ(ctx.mappedKvStateId, -1);
    EXPECT_EQ(ctx.valueSerializer, nullptr);
    EXPECT_EQ(ctx.stateType, omnistream::VectorBatchStateType::KV);
    EXPECT_FALSE(ctx.writable);
}

TEST(VectorBatchSaveTest, SaveStateContextMoveAssignmentClosesTargetAndResetsSource)
{
    auto accessor = std::make_shared<MockVectorBatchStateAccessor>();
    MockSerializer serializer;

    omnistream::VectorBatchSaveStateContext ctx;
    ctx.writable = true;
    ctx.mappedKvStateId = 3;
    ctx.logicalStateName = "sourceState";
    ctx.valueSerializer = &serializer;
    ctx.vbAccessor = accessor;
    ctx.stateType = omnistream::VectorBatchStateType::KV_WITH_VB;

    omnistream::VectorBatchSaveStateContext target;
    target = std::move(ctx);

    // 目标对象应正确获取所有字段
    EXPECT_TRUE(target.isValid());
    EXPECT_EQ(target.mappedKvStateId, 3);
    EXPECT_EQ(target.logicalStateName, "sourceState");
    EXPECT_EQ(target.valueSerializer, &serializer);
    EXPECT_EQ(target.vbAccessor.get(), accessor.get());
    EXPECT_EQ(target.stateType, omnistream::VectorBatchStateType::KV_WITH_VB);

    // 源对象应被重置为默认值
    EXPECT_FALSE(ctx.isValid());
    EXPECT_EQ(ctx.mappedKvStateId, -1);
    EXPECT_EQ(ctx.valueSerializer, nullptr);
    EXPECT_EQ(ctx.stateType, omnistream::VectorBatchStateType::KV);
    EXPECT_FALSE(ctx.writable);
}

TEST(VectorBatchSaveTest, SaveStateContextMoveAssignmentSelfAssignmentSafe)
{
    auto accessor = std::make_shared<MockVectorBatchStateAccessor>();
    MockSerializer serializer;

    omnistream::VectorBatchSaveStateContext ctx;
    ctx.writable = true;
    ctx.mappedKvStateId = 7;
    ctx.logicalStateName = "self";
    ctx.valueSerializer = &serializer;
    ctx.vbAccessor = accessor;
    ctx.stateType = omnistream::VectorBatchStateType::PQ;

    // 自我赋值安全：通过将同一对象的地址赋给引用，再 move 赋值
    // 注意：由于 &ctx = &ctx 恒成立，operator= 的 this != &other 检查应跳过。
    ctx = std::move(ctx);

    EXPECT_TRUE(ctx.isValid());
    EXPECT_EQ(ctx.mappedKvStateId, 7);
}

// ============================================================================
// VectorBatchSaveHooks — 默认方法实现
// ============================================================================

TEST(VectorBatchSaveTest, DefaultEncodeFlinkLogicalValueReturnsEmpty)
{
    const auto key = bytes({0x10, 0x20});
    const auto value = bytes({0x01, 0x02, 0x03});
    KeyValueStateIterator::CurrentEntry entry;
    entry.key = ByteView(key.data(), key.size());
    entry.value = ByteView(value.data(), value.size());

    MinimalHooks hooks;
    MockRowData row;
    omnistream::VectorBatchSaveStateContext context;
    omnistream::VectorBatchSavePlan plan;

    // 默认实现应返回空 vector
    std::vector<int8_t> result = hooks.encodeFlinkLogicalValue(entry, row, context, plan);
    EXPECT_TRUE(result.empty());
}

TEST(VectorBatchSaveTest, DefaultParseVectorBatchReferenceReturnsMinusOne)
{
    const auto value = bytes({0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08});
    KeyValueStateIterator::CurrentEntry entry;
    entry.value = ByteView(value.data(), value.size());

    MinimalHooks hooks;
    omnistream::VectorBatchSaveStateContext context;
    omnistream::VectorBatchSavePlan plan;

    // 默认实现应返回 -1
    EXPECT_EQ(hooks.parseVectorBatchReference(entry.value, context, plan), -1);
}
