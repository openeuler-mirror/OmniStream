#include <gtest/gtest.h>
#include "core/fs/Path.h"
#include "runtime/executiongraph/JobIDPOD.h"
#include "runtime/buffer/ObjectBuffer.h"
#include "runtime/state/CheckpointStorage.h"
#include "runtime/state/filesystem/FsCheckpointStorageAccess.h"
#include "runtime/checkpoint/channel/ChannelStateSerializer.h"
#include "runtime/checkpoint/channel/ChannelStateWriteRequestDispatcherImpl.h"

using namespace omnistream;

class ChannelStateSerializerImplTest : public ChannelStateSerializerImpl {
public:
    void WriteHeader(std::ostringstream& dataStream) override { 
        writeHeaderCalled = true; 
    }
    
    void WriteData(std::ostringstream& dataStream, const ObjectBuffer& buffer) override { 
        writeDataCalled = true; 
    }
    
    int64_t GetHeaderLength() const override { return 999; }

    bool writeHeaderCalled = false;
    bool writeDataCalled = false;
};

class CheckpointStorageTest : public CheckpointStorage {
public:
    CheckpointStorageAccess* createCheckpointStorage(const JobIDPOD& jobId) override {
        // const std::string dir = std::filesystem::temp_directory_path().string();
        const std::string dir = "";

        Path* checkpointDir = new Path(dir);
        Path* savepointDir = new Path(dir);
        
        return new FsCheckpointStorageAccess(
            checkpointDir,
            savepointDir,
            jobId,
            100,
            100);
    }
};

class ObjectBufferTest : public ObjectBuffer {
public:
    bool isBuffer() const override { return true; }
    std::shared_ptr<BufferRecycler> GetRecycler() override {
        return DummyObjectBufferRecycler::getInstance(); 
    }
    void RecycleBuffer() override { recycled = true; }
    bool IsRecycled() const override { return recycled; }
    std::shared_ptr<Buffer> RetainBuffer() override {
        return std::make_shared<ObjectBufferTest>(); 
    }
    std::shared_ptr<Buffer> ReadOnlySlice() override {
        return std::make_shared<ObjectBufferTest>(); 
    }
    std::shared_ptr<Buffer> ReadOnlySlice(int index, int length) override {
        return std::make_shared<ObjectBufferTest>(); 
    }
    int GetMaxCapacity() const override { return 1024; }
    int GetReaderIndex() const override { return 0; }
    void SetReaderIndex(int readerIndex) override {}
    int GetSize() const override { return 0; }
    void SetSize(int writerIndex) override {}
    int ReadableObjects() const override { return 0; }
    bool IsCompressed() const override { return false; }
    void SetCompressed(bool isCompressed) override {}
    ObjectBufferDataType GetDataType() const override { return ObjectBufferDataType::DATA_BUFFER; }
    void SetDataType(ObjectBufferDataType dataType) override {}
    int RefCount() const override { return 1; }
    std::string ToDebugString(bool includeHash) const override { return "ObjectBufferTest"; }
    std::shared_ptr<ObjectSegment> GetObjectSegment() override { 
        return std::make_shared<ObjectSegment>(0); 
    }
    int GetBufferType() override {return 42;}
    std::pair<uint8_t *, size_t> GetBytes() override { 
        return {nullptr, 0}; 
    }

private:
    bool recycled = false;
};

TEST(ChannelStateWriteRequestDispatcherImplTest, InitialiseRequestDispatcher) {
    auto storage = new CheckpointStorageTest();
    auto serializer = new ChannelStateSerializerImplTest();
    auto dispatcher = new ChannelStateWriteRequestDispatcherImpl(
        storage,
        JobIDPOD(-1, -1),
        serializer
    );

    JobVertexID jobVertexID(-1,-1);
    ChannelStateWriter::ChannelStateWriteResult targetResult;
    CheckpointStorageLocationReference locationReference;

    auto registerRequest = new SubtaskRegisterRequest(jobVertexID, 1);
    dispatcher->dispatch(*registerRequest);

    auto startRequest = new CheckpointStartRequest(
        jobVertexID,
        1,
        0,
        targetResult,
        locationReference
    );

    dispatcher->dispatch(*startRequest);

    EXPECT_TRUE(serializer->writeHeaderCalled);

    auto releaseRequest = new SubtaskReleaseRequest(jobVertexID, 1);
    dispatcher->dispatch(*releaseRequest);

    delete registerRequest;
    delete startRequest;
    delete dispatcher;
    delete serializer;
    delete storage;
}

TEST(ChannelStateWriteRequestDispatcherImplTest, AbortedCheckpointIsCancelledNotThrown) {
    auto storage = new CheckpointStorageTest();
    auto serializer = new ChannelStateSerializerImplTest();
    auto dispatcher = new ChannelStateWriteRequestDispatcherImpl(
        storage, JobIDPOD(-1, -1), serializer);

    JobVertexID jvid(-1,-1);
    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();

    dispatcher->dispatch(*ChannelStateWriteRequest::registerSubtask(jvid, 1));
    auto startRequest = ChannelStateWriteRequest::start(jvid, 1, 1, targetResult, {});
    dispatcher->dispatch(*startRequest);

    auto oldStartRequest = ChannelStateWriteRequest::start(
            jvid,
            1,
            0,
            targetResult,
            {});

    EXPECT_NO_THROW(dispatcher->dispatch(*oldStartRequest));
    EXPECT_TRUE(targetResult.GetInputChannelStateHandles()->IsCancelled());
}

TEST(ChannelStateWriteRequestDispatcherImplTest, FullRequestFlow) {
    auto storage = new CheckpointStorageTest();
    auto serializer = new ChannelStateSerializerImplTest();
    auto dispatcher = new ChannelStateWriteRequestDispatcherImpl(
        storage,
        JobIDPOD(-1, -1),
        serializer
    );

    JobVertexID jobVertexID(-1,-1);
    auto targetResult = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    CheckpointStorageLocationReference locationReference;

    auto registerRequest = new SubtaskRegisterRequest(jobVertexID, 1);
    dispatcher->dispatch(*registerRequest);

    auto startRequest = new CheckpointStartRequest(
        jobVertexID,
        1,
        1,
        targetResult,
        locationReference
    );
    dispatcher->dispatch(*startRequest);
    EXPECT_TRUE(serializer->writeHeaderCalled);

    auto completeInputRequest = ChannelStateWriteRequest::completeInput(jobVertexID, 1, 1);
    dispatcher->dispatch(*completeInputRequest);

    auto completeOutputRequest = ChannelStateWriteRequest::completeOutput(jobVertexID, 1, 1);
    dispatcher->dispatch(*completeOutputRequest);

    auto releaseRequest = new SubtaskReleaseRequest(jobVertexID, 1);
    dispatcher->dispatch(*releaseRequest);

    auto oldCheckpointRequest = ChannelStateWriteRequest::completeInput(jobVertexID, 1, 0);
    dispatcher->dispatch(*oldCheckpointRequest);
    EXPECT_FALSE(targetResult.IsDone());

    auto abortRequest = ChannelStateWriteRequest::terminate(
        jobVertexID,
        1,
        1,
        std::make_exception_ptr(std::runtime_error("Test error")));
    dispatcher->dispatch(*abortRequest);
    EXPECT_TRUE(targetResult.GetInputChannelStateHandles()->IsCancelled());
    EXPECT_TRUE(targetResult.GetResultSubpartitionStateHandles()->IsCancelled());

    auto invalidStartRequest = new CheckpointStartRequest(
        jobVertexID,
        1,
        0,
        targetResult,
        locationReference
    );
    dispatcher->dispatch(*invalidStartRequest);
    EXPECT_TRUE(targetResult.GetInputChannelStateHandles()->IsCancelled());

    auto unregisterRequest = new SubtaskReleaseRequest(jobVertexID, 2);
    EXPECT_NO_THROW(dispatcher->dispatch(*unregisterRequest));
    
    delete registerRequest;
    delete startRequest;
    delete releaseRequest;
    delete invalidStartRequest;
    delete unregisterRequest;
    delete dispatcher;
    delete serializer;
    delete storage;
}