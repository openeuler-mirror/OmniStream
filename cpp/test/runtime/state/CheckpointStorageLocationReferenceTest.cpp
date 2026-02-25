#include <gtest/gtest.h>
#include <memory>
#include "runtime/state/CheckpointStorageLocationReference.h"

TEST(CheckpointStorageLocationReferenceTest, DefaultTest)
{
    auto ref = CheckpointStorageLocationReference::GetDefault();
    EXPECT_TRUE(ref->IsDefaultReference());
    EXPECT_EQ(ref->HashCode(), 2059243550);
}

TEST(CheckpointStorageLocationReferenceTest, InitTest)
{
    std::vector<uint8_t> bytes = {1, 2, 3, 4, 5};
    auto encodedReference = std::make_shared<std::vector<uint8_t>>(bytes);
    auto ref = std::make_shared<CheckpointStorageLocationReference>(encodedReference);

    EXPECT_FALSE(ref->IsDefaultReference());
    EXPECT_EQ(*(ref->GetReferenceBytes()), *encodedReference);
}