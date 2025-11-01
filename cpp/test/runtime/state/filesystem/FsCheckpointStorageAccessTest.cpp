#include <gtest/gtest.h>
#include "runtime/state/filesystem/FsCheckpointStorageAccess.h"

using namespace omnistream;

TEST(FsCheckpointStorageAccessTest, InitTest)
{
    Path checkpointBaseDirectory("/tmp/checkpoints");
    Path defaultSavepointDirectory("/tmp/savepoints");
    JobIDPOD jobId(12345, 122222);
    int fileSizeThreshold = 1024;
    int writeBufferSize = 4096;

    auto storage = new FsCheckpointStorageAccess(
        &checkpointBaseDirectory,
        &defaultSavepointDirectory,
        jobId,
        fileSizeThreshold,
        writeBufferSize
    );
}