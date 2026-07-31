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

    void convertKVRowData(
        const KeyValueStateIterator::CurrentEntry& /*entry*/,
        const omnistream::VectorBatchSaveStateContext& /*context*/,
        const omnistream::VectorBatchSavePlan& /*plan*/,
        std::function<void(omnistream::ConvertedEntry)> /*output*/) override
    {
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

    context.stateType = omnistream::VectorBatchStateType::KV_LIST_WITH_VB;
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
