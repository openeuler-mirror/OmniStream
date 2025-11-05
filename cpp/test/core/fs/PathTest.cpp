#include <gtest/gtest.h>
#include "core/fs/Path.h"

TEST(PathTest, JoinTwoPaths)
{
    auto parent = new Path("/path/to/file/");
    auto child = new Path("/path/to/child/");
    auto joined = new Path(*parent, *child);
    EXPECT_EQ(joined->toString(), "/path/to/file/path/to/child/");
}

