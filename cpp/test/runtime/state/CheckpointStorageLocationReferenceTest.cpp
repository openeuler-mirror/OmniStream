#include <gtest/gtest.h>

#include "runtime/state/CheckpointStorageLocationReference.h"

TEST(CheckpointStorageLocationReferenceTest, DefaultTest)
{
    auto ref = CheckpointStorageLocationReference::GetDefault();
    EXPECT_TRUE(ref->IsDefaultReference());
    EXPECT_EQ(ref->HashCode(), 2059243550);
}

TEST(CheckpointStorageLocationReferenceTest, InitTest)
{
    auto encodedReference = new std::vector<uint8_t>{1, 2, 3, 4, 5};
    auto ref = new CheckpointStorageLocationReference(encodedReference);

    EXPECT_FALSE(ref->IsDefaultReference());
    EXPECT_EQ(*(ref->GetReferenceBytes()), *encodedReference);

    delete ref;
}