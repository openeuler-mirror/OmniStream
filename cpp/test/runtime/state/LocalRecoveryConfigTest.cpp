#include "gtest/gtest.h"
#include "runtime/state/LocalRecoveryConfig.h"
#include "runtime/state/LocalRecoveryDirectoryProviderImpl.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/jobgraph/JobVertexID.h"
#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

using namespace omnistream;

TEST(LocalRecoveryConfigTest, DisabledLocalRecovery) {
    LocalRecoveryConfig config(nullptr);
    
    EXPECT_FALSE(config.IsLocalRecoveryEnabled());
    EXPECT_EQ(config.GetLocalStateDirectoryProvider(), nullptr);
    EXPECT_EQ(config.ToString(), "LocalRecoveryConfig{localStateDirectories=null}");
}

TEST(LocalRecoveryConfigTest, EnabledLocalRecovery) {
    fs::path tempDir = fs::temp_directory_path() / "LocalRecoveryConfigTest";
    fs::create_directories(tempDir);
    
    JobIDPOD jobID(1, 1);
    JobVertexID jobVertexID(1, 1);
    std::vector<fs::path> baseDirs = { tempDir / "EnabledLocalRecovery" };
    
    auto provider = std::make_shared<LocalRecoveryDirectoryProviderImpl>(baseDirs, jobID, jobVertexID, 0);
    LocalRecoveryConfig config(provider);
    
    EXPECT_TRUE(config.IsLocalRecoveryEnabled());
    EXPECT_NE(config.GetLocalStateDirectoryProvider(), nullptr);
    EXPECT_NE(config.ToString().find(baseDirs[0].string()), std::string::npos);
    
    fs::remove_all(tempDir);
}
