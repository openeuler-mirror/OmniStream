#include <gtest/gtest.h>
#include "runtime/executiongraph/JobIDPOD.h"

TEST(JobIDPODTest, CopyTest)
{
    auto id = new omnistream::JobIDPOD();
    auto bytes = id->getBytes();
    auto copy = omnistream::JobIDPOD::fromByteArray(bytes);
    EXPECT_EQ(*id, *copy);
}