#include <gtest/gtest.h>
#include "runtime/buffer/ObjectBufferBuilder.h"
#include "runtime/buffer/ObjectBufferRecycler.h"
#include "runtime/buffer/ObjectSegment.h"
#include "core/streamrecord/StreamRecord.h"

using namespace omnistream;
TEST(ObjectBufferBuilderTest, AppendAndCommintNotFull)
{

    int size = 10;
    auto objSegment = std::make_shared<ObjectSegment>(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder *bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size - 1; i++)
    {
        StreamRecord *record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }

    std::cout << "max size = " << bufferBuilder->getMaxCapacity();

    EXPECT_EQ(bufferBuilder->getMaxCapacity(), size);
    EXPECT_EQ(bufferBuilder->isFull(), false);
}

TEST(ObjectBufferBuilderTest, AppendAndCommintFull)
{

    int size = 10;
    auto objSegment = std::make_shared<ObjectSegment>(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder *bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size; i++)
    {
        StreamRecord *record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }

    std::cout << "max size = " << bufferBuilder->getMaxCapacity();

    EXPECT_EQ(bufferBuilder->getMaxCapacity(), size);
    EXPECT_EQ(bufferBuilder->isFull(), true);
}

TEST(ObjectBufferBuilderTest, AppendAndCommintExceed)
{

    int size = 10;
    auto objSegment = std::make_shared<ObjectSegment>(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder *bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    // Append size + 1 records to exceed the buffer capacity
    try
    {
        for (int i = 0; i < size + 1; i++)
        {
            StreamRecord *record = new StreamRecord();
            auto v = new VectorBatch(1);
            record->setValue(v);
            bufferBuilder->append(record);
        }
    }
    catch (const std::runtime_error &e)
    {
        EXPECT_STREQ(e.what(), "BufferBuilder is finished");
    }
}

TEST(ObjectBufferBuilderTest, Finish)
{

    int size = 10;
    auto objSegment = std::make_shared<ObjectSegment>(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder *bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size; i++)
    {
        StreamRecord *record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }
    bufferBuilder->finish();
    EXPECT_EQ(bufferBuilder->isFinished(), true);
}

TEST(ObjectBufferBuilderTest, Recycle)
{

    int size = 10;
    auto objSegment = std::make_shared<ObjectSegment>(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder *bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    for (int i = 0; i < size; i++)
    {
        StreamRecord *record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        bufferBuilder->append(record);
    }
    bufferBuilder->close();
}

TEST(ObjectBufferBuilderTest, BufferConsumerReadable)
{

    int size = 10;
    auto objSegment = std::make_shared<ObjectSegment>(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder *bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    std::shared_ptr<ObjectBufferConsumer> bufferConsumer = bufferBuilder->createBufferConsumer();

    for (int i = 0; i < size; i++)
    {
        StreamRecord *record = new StreamRecord();
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
    // EXPECT_EQ(bufferConsumer->IsRecycled(), true);
}

TEST(ObjectBufferBuilderTest, BufferConsumerDataIdentical)
{

    int size = 10;
    auto objSegment = std::make_shared<ObjectSegment>(size);

    std::shared_ptr<DummyObjectBufferRecycler> recycler = DummyObjectBufferRecycler::getInstance();

    ObjectBufferBuilder *bufferBuilder = new ObjectBufferBuilder(objSegment, recycler);

    std::shared_ptr<ObjectBufferConsumer> bufferConsumer = bufferBuilder->createBufferConsumer();

    StreamRecord **objects = new StreamRecord *[size];
    for (int i = 0; i < size; i++)
    {
        StreamRecord *record = new StreamRecord();
        auto v = new VectorBatch(1);
        record->setValue(v);
        objects[i] = record;
        bufferBuilder->appendAndCommit(record);
    }
    std::shared_ptr<VectorBatchBuffer> readBuffer = bufferConsumer->build();
    int readSize = readBuffer->GetSize();
    EXPECT_EQ(readSize, size);
    for (size_t i = 0; i < readSize; i++)
    {
        StreamRecord *record = static_cast<StreamRecord*>(readBuffer->GetObjectSegment()->getObject(i));
        EXPECT_EQ(record, objects[i]);
    }
}