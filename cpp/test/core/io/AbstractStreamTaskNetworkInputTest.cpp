#include <gtest/gtest.h>
#include "core/io/AbstractStreamTaskNetworkInput.h"
#include "core/typeutils/LongSerializer.h"
#include "core/io/RecordDeserializer.h"
#include "core/io/DeserializationResult.h"
#include "core/io/DataOutput.h"
#include "functions/StreamElement.h"
#include "core/streamrecord/StreamRecord.h"
#include "basictypes/String.h"
#include <cstdint>
#include <memory>

using namespace std;

class PrintDataOutput : public DataOutput {
    void emitRecord(StreamRecord *streamRecord) override {
        std::cout << "PrintDataOutput emitRecord:" << reinterpret_cast<String*>(streamRecord->getValue()) << std::endl;
    }
};

TEST(AbstractStreamTaskNetworkInputTest, Constructor) {
    LongSerializer typeSerializer;
    auto recordDeserializers = std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();
    recordDeserializers->emplace(1, new omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer());

    omnistream::datastream::AbstractStreamTaskNetworkInput input(&typeSerializer, std::move(recordDeserializers));
    EXPECT_NE(input.getActiveSerializer(1), nullptr);
    EXPECT_EQ(input.getActiveSerializer(2), nullptr);
}

TEST(AbstractStreamTaskNetworkInputTest, Destructor) {
    LongSerializer typeSerializer;
    auto recordDeserializers = std::make_unique<std::unordered_map<long,  omnistream::datastream::RecordDeserializer*>>();
    recordDeserializers->emplace(1, new  omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer());

    omnistream::datastream::AbstractStreamTaskNetworkInput* input = new  omnistream::datastream::AbstractStreamTaskNetworkInput(&typeSerializer, std::move(recordDeserializers));
    delete input;
    // No specific expectations, just ensure no memory leaks
}

TEST(AbstractStreamTaskNetworkInputTest, EmitNextProcessElement_BufferConsumed) {
    LongSerializer typeSerializer;
    auto recordDeserializers = std::make_unique<std::unordered_map<long,  omnistream::datastream::RecordDeserializer*>>();
    omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer* deserializer = new  omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer();
    recordDeserializers->emplace(1, deserializer);

    omnistream::datastream::AbstractStreamTaskNetworkInput input(&typeSerializer, std::move(recordDeserializers));
    PrintDataOutput output;
    int32_t inputNumber = 0;

    std::string str = "dummy";
    auto data = reinterpret_cast<uint8_t *>(str.data());
    input.emitNextProcessBuffer(data, 6, 1);
    uint32_t status = input.emitNextProcessElement(output, inputNumber);

    EXPECT_TRUE(status & OmniDataInputStatus::BUFFER_CONSUMED_TRUE);
}

TEST(AbstractStreamTaskNetworkInputTest, EmitNextProcessBuffer_InvalidChannelInfo) {
    LongSerializer typeSerializer;
    auto recordDeserializers = std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();

    omnistream::datastream::AbstractStreamTaskNetworkInput input(&typeSerializer, std::move(recordDeserializers));

    std::string str = "dummy";
    auto data = reinterpret_cast<uint8_t *>(str.data());
    EXPECT_THROW(input.emitNextProcessBuffer(data, 6, -1), std::logic_error);
}

TEST(AbstractStreamTaskNetworkInputTest, EmitNextProcessBuffer_ChannelInfoNotPresent) {
    LongSerializer typeSerializer;
    auto recordDeserializers = std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();

    omnistream::datastream::AbstractStreamTaskNetworkInput input(&typeSerializer, std::move(recordDeserializers));

    std::string str = "dummy";
    auto data = reinterpret_cast<uint8_t *>(str.data());
    EXPECT_THROW(input.emitNextProcessBuffer(data, 6, 1), std::logic_error);
}

TEST(AbstractStreamTaskNetworkInputTest, ProcessElement_RecordWithoutTimestamp) {
    LongSerializer typeSerializer;
    auto recordDeserializers = std::make_unique<std::unordered_map<long, omnistream::datastream::RecordDeserializer*>>();
    recordDeserializers->emplace(1, new omnistream::datastream::SpillingAdaptiveSpanningRecordDeserializer());

    omnistream::datastream::AbstractStreamTaskNetworkInput input(&typeSerializer, std::move(recordDeserializers));
    PrintDataOutput output;
    int32_t inputNumber = 0;

    String str("10");
    StreamRecord record(&str);
    record.setTag(StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP);
    bool breakBatchEmitting = input.processElement(&record, output, inputNumber);
    EXPECT_EQ(inputNumber, 1);
    EXPECT_FALSE(breakBatchEmitting);
}