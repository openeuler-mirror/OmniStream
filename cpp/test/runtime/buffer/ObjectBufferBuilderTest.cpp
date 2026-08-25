#include <gtest/gtest.h>
#include <cstdint>
#include <cstring>
#include <vector>
#include "runtime/buffer/ObjectBufferBuilder.h"
#include "runtime/buffer/ObjectBufferRecycler.h"
#include "runtime/buffer/ObjectSegment.h"
#include "streaming/runtime/streamrecord/StreamRecord.h"

using namespace omnistream;

TEST(ObjectBufferBuilderTest, AppendSerializedObjectSegmentReleasesWrittenObjectsOnFailure)
{
    constexpr int segmentSize = 3;
    ObjectSegment target(segmentSize);
    StreamElement existing;
    target.putObject(0, &existing);

    int32_t elementNum = 2;
    int8_t watermarkTag = static_cast<int8_t>(StreamElementTag::TAG_WATERMARK);
    long timestamp = 123;
    int8_t unsupportedTag = static_cast<int8_t>(StreamElementTag::TAG_STREAM_STATUS);
    std::vector<uint8_t> serialized(
        sizeof(elementNum) + sizeof(watermarkTag) + sizeof(timestamp) + sizeof(unsupportedTag));
    uint8_t* cursor = serialized.data();
    std::memcpy(cursor, &elementNum, sizeof(elementNum));
    cursor += sizeof(elementNum);
    std::memcpy(cursor, &watermarkTag, sizeof(watermarkTag));
    cursor += sizeof(watermarkTag);
    std::memcpy(cursor, &timestamp, sizeof(timestamp));
    cursor += sizeof(timestamp);
    std::memcpy(cursor, &unsupportedTag, sizeof(unsupportedTag));

    try {
        ObjectSegmentChannelStateSerde::AppendSerializedObjectSegment(
            serialized.data(), static_cast<int>(serialized.size()), &target, 1, 2);
        FAIL() << "Expected unsupported StreamElement tag to throw";
    } catch (const std::runtime_error& error) {
        EXPECT_STREQ(error.what(), "ObjectSegment channel-state deserialization does not support StreamElement tag 4");
    }
    EXPECT_EQ(target.getObject(0), &existing);
    EXPECT_EQ(target.getObject(1), nullptr);
    EXPECT_EQ(target.getObject(2), nullptr);
}

TEST(ObjectBufferBuilderTest, AppendAndCommintNotFull)
{
    int size = 10;
    auto objSegment = new ObjectSegment(size);
    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder* bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size - 1; i++) {
        StreamRecord* record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }

    std::cout << "max size = " << bufferBuilder->getMaxCapacity();

    EXPECT_EQ(bufferBuilder->getMaxCapacity(), size);
    EXPECT_EQ(bufferBuilder->isFull(), false);
    delete objSegment;
    delete bufferBuilder;
}

TEST(ObjectBufferBuilderTest, AppendAndCommintFull)
{
    int size = 10;
    auto objSegment = new ObjectSegment(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder* bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size; i++) {
        StreamRecord* record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }

    std::cout << "max size = " << bufferBuilder->getMaxCapacity();

    EXPECT_EQ(bufferBuilder->getMaxCapacity(), size);
    EXPECT_EQ(bufferBuilder->isFull(), true);
    delete objSegment;
    delete bufferBuilder;
}

TEST(ObjectBufferBuilderTest, AppendAndCommintExceed)
{
    int size = 10;
    auto objSegment = new ObjectSegment(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder* bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    // Append size + 1 records to exceed the buffer capacity
    try {
        for (int i = 0; i < size + 1; i++) {
            StreamRecord* record = new StreamRecord();
            auto v = new VectorBatch(1);
            record->setValue(v);
            bufferBuilder->append(record);
        }
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ(e.what(), "BufferBuilder is finished");
    }
    delete objSegment;
    delete bufferBuilder;
}

TEST(ObjectBufferBuilderTest, Finish)
{
    int size = 10;
    auto objSegment = new ObjectSegment(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder* bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size; i++) {
        StreamRecord* record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }
    bufferBuilder->finish();
    EXPECT_EQ(bufferBuilder->isFinished(), true);
    delete objSegment;
    delete bufferBuilder;
}

TEST(ObjectBufferBuilderTest, Recycle)
{
    int size = 10;
    auto objSegment = new ObjectSegment(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder* bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size; i++) {
        StreamRecord* record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }
    bufferBuilder->close();
    delete objSegment;
    delete bufferBuilder;
}

TEST(ObjectBufferBuilderTest, BufferConsumerReadable)
{
    int size = 10;
    auto objSegment = new ObjectSegment(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder* bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    std::shared_ptr<ObjectBufferConsumer> bufferConsumer =
        std::dynamic_pointer_cast<ObjectBufferConsumer>(bufferBuilder->createBufferConsumer());

    for (int i = 0; i < size; i++) {
        StreamRecord* record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->appendAndCommit(record);
    }

    int currentReadIndex = bufferConsumer->getCurrentReaderPosition();
    EXPECT_EQ(currentReadIndex, 0);
    EXPECT_EQ(bufferConsumer->isDataAvailable(), true);

    bufferConsumer->build();
    int readableBytes = bufferConsumer->getWrittenBytes();
    EXPECT_EQ(readableBytes, size);
    EXPECT_EQ(bufferConsumer->isDataAvailable(), false);
    // bufferConsumer->close();
    delete objSegment;
    delete bufferBuilder;
}

TEST(ObjectBufferBuilderTest, BufferConsumerDataIdentical)
{
    int size = 10;
    auto objSegment = new ObjectSegment(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder* bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    std::shared_ptr<ObjectBufferConsumer> bufferConsumer =
        std::dynamic_pointer_cast<ObjectBufferConsumer>(bufferBuilder->createBufferConsumer());

    StreamRecord** objects = new StreamRecord*[size];
    for (int i = 0; i < size; i++) {
        StreamRecord* record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        objects[i] = record;
        bufferBuilder->appendAndCommit(record);
    }
    VectorBatchBuffer* readBuffer = dynamic_cast<VectorBatchBuffer*>(bufferConsumer->build());
    int readSize = readBuffer->GetSize();
    EXPECT_EQ(readSize, size);
    for (size_t i = 0; i < readSize; i++) {
        StreamRecord* record = static_cast<StreamRecord*>(readBuffer->GetObjectSegment()->getObject(i));
        EXPECT_EQ(record, objects[i]);
    }
    delete readBuffer;
}
