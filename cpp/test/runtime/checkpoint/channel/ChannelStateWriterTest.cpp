#include <gtest/gtest.h>
#include "runtime/checkpoint/channel/ChannelStateWriter.h"
#include "core/utils/threads/CompletableFutureV2.h"

using namespace omnistream;

TEST(ChannelStateWriteResultTest, CreateEmpty)
{
    auto result = ChannelStateWriter::ChannelStateWriteResult::CreateEmpty();
    EXPECT_FALSE(result->IsDone());
    
    result->Fail(std::make_exception_ptr(std::runtime_error("Test failure")));
    EXPECT_TRUE(result->GetInputChannelStateHandles()->IsCancelled());
    EXPECT_TRUE(result->GetResultSubpartitionStateHandles()->IsCancelled());
}

TEST(ChannelStateWriteResultTest, IsDone)
{
    auto inputFuture = std::make_shared<CompletableFutureV2<
        ChannelStateWriter::ChannelStateWriteResult::InputChannelStateHandleVecPtr>>();
    auto resultFuture = std::make_shared<CompletableFutureV2<
        ChannelStateWriter::ChannelStateWriteResult::ResultSubpartitionStateVecPtr>>();

    ChannelStateWriter::ChannelStateWriteResult result(inputFuture, resultFuture);
    EXPECT_FALSE(result.IsDone());

    inputFuture->Complete(std::make_shared<std::vector<InputChannelStateHandle>>());
    EXPECT_FALSE(result.IsDone());

    resultFuture->Complete(std::make_shared<std::vector<ResultSubpartitionStateHandle>>());
    EXPECT_TRUE(result.IsDone());
}
