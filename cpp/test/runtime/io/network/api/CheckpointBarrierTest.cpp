#include <gtest/gtest.h>
#include "runtime/io/network/api/CheckpointBarrier.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
TEST(CheckpointBarrierTest, ConstructorTest)
{
    auto checkpointType = CheckpointType::CHECKPOINT;
    auto targetLocation = CheckpointStorageLocationReference::GetDefault();
    auto options = new CheckpointOptions(checkpointType, targetLocation);

    CheckpointBarrier barrier(1, 1000, options);

    EXPECT_EQ(barrier.GetId(), 1);
    EXPECT_EQ(barrier.GetTimestamp(), 1000);
    EXPECT_EQ(barrier.GetCheckpointOptions(), options);
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithDefaultLocation)
{
    CheckpointOptions* options = new CheckpointOptions(
        CheckpointType::CHECKPOINT,
        CheckpointStorageLocationReference::GetDefault()
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSpecLocation)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    CheckpointOptions* options = new CheckpointOptions(
        CheckpointType::CHECKPOINT,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithFullCheckpoint)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    CheckpointOptions* options = new CheckpointOptions(
        CheckpointType::FULL_CHECKPOINT,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}


TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointNoneCanonical)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    SavepointType* savepoint = SavepointType::savepoint(SavepointFormatType::CANONICAL);
    CheckpointOptions* options = new CheckpointOptions(
        savepoint,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}


TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointNoneNative)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    SavepointType* savepoint = SavepointType::savepoint(SavepointFormatType::NATIVE);
    CheckpointOptions* options = new CheckpointOptions(
        savepoint,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointTerminateNative)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    SavepointType* savepoint = SavepointType::terminate(SavepointFormatType::NATIVE);
    CheckpointOptions* options = new CheckpointOptions(
        savepoint,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointTerminateCanonical)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    SavepointType* savepoint = SavepointType::terminate(SavepointFormatType::CANONICAL);
    CheckpointOptions* options = new CheckpointOptions(
        savepoint,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointSuspendCanonical)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    SavepointType* savepoint = SavepointType::suspend(SavepointFormatType::CANONICAL);
    CheckpointOptions* options = new CheckpointOptions(
        savepoint,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}

TEST(CheckpointBarrierTest, serializeDeserializeTestWithSavePointSuspendNative)
{
    std::vector<uint8_t>* refBytes = new std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04};
    CheckpointStorageLocationReference location = CheckpointStorageLocationReference(refBytes);
    SavepointType* savepoint = SavepointType::suspend(SavepointFormatType::NATIVE);
    CheckpointOptions* options = new CheckpointOptions(
        savepoint,
        &location
    );
    CheckpointBarrier barrier(1, 1000, options);

    auto bufferConsumer = EventSerializer::ToBufferConsumer(std::make_shared<CheckpointBarrier>(barrier), false);
    auto deserializedEvent = EventSerializer::fromSerializedEvent(bufferConsumer->build());
    auto desCheckpointBarrier = std::dynamic_pointer_cast<CheckpointBarrier>(deserializedEvent);

    EXPECT_EQ(barrier.GetId(), desCheckpointBarrier->GetId());
    EXPECT_EQ(barrier.GetTimestamp(), desCheckpointBarrier->GetTimestamp());
    EXPECT_EQ(*barrier.GetCheckpointOptions(), *desCheckpointBarrier->GetCheckpointOptions());

    delete options;
}
