#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "core/fs/Path.h"
#include "runtime/state/filesystem/FsCheckpointStateOutputStream.h"

namespace fs = std::filesystem;

TEST(FsCheckpointStateOutputStream, WriteAndGetPos) {
    std::string baseName = (fs::temp_directory_path() / "Test").string();

    FsCheckpointStateOutputStream stream(
        Path(baseName),
        0,
        0,
        0,
        true
    );

    const char data[] = "xyz";
    stream.Write(data, 3);
    EXPECT_EQ(stream.GetPos(), 3);

    stream.Flush();

    std::ifstream in(baseName + ".tmp", std::ios::binary);
    ASSERT_TRUE(in.good());

    std::string contents((std::istreambuf_iterator<char>(in)),
                          std::istreambuf_iterator<char>());
    EXPECT_EQ(contents, "xyz");
    EXPECT_FALSE(stream.IsClosed());

    in.close();
    fs::remove(baseName + ".tmp");
}
