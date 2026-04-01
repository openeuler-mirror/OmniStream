#include <gtest/gtest.h>

#include "runtime/buffer/LocalObjectBufferPool.h"
#include "runtime/buffer/NetworkObjectBufferPool.h"

using namespace omnistream;

TEST(NetworkObjectBufferPoolTest, DirectRequestRecycleUsesObjectMemory)
{
    int segmentNum = 3;
    int segmentSize = 10;
    auto networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);
    EXPECT_EQ(networkObjectBufferPool->getAvailableMemory(), segmentNum * segmentSize);

    auto segment = networkObjectBufferPool->requestPooledObjectSegment(segmentSize);
    ASSERT_NE(segment, nullptr);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum - 1);
    EXPECT_EQ(networkObjectBufferPool->getAvailableMemory(), (segmentNum - 1) * segmentSize);

    networkObjectBufferPool->recyclePooledObjectSegment(segment);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);
    EXPECT_EQ(networkObjectBufferPool->getAvailableMemory(), segmentNum * segmentSize);
}

TEST(LocalObjectBufferPoolTest, RequestBuffer)
{
    int segmentNum = 100;
    int segmentSize = 10;
    auto networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    int requiredBufferNum = 5;
    int maxBufferAllowedToRequest = 5;
    auto localObjectBufferPool =
        std::make_shared<LocalObjectBufferPool>(networkObjectBufferPool, requiredBufferNum, maxBufferAllowedToRequest);

    EXPECT_EQ(localObjectBufferPool->getRequiredMemory(), static_cast<uint64_t>(requiredBufferNum * segmentSize));

    std::shared_ptr<Buffer> buffer1 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer1, nullptr);

    std::shared_ptr<Buffer> buffer2 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer3 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer2, nullptr);
    ASSERT_NE(buffer3, nullptr);

    std::shared_ptr<Buffer> buffer4 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer5 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer4, nullptr);
    ASSERT_NE(buffer5, nullptr);

    std::shared_ptr<Buffer> buffer6 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer6, nullptr);

    // buffer1->RecycleBuffer();

    std::shared_ptr<Buffer> buffer7 = localObjectBufferPool->requestBuffer();
    ASSERT_NE(buffer7, nullptr);
}

TEST(LocalObjectBufferPoolTest, DISABLED_Recycle)
{
    int segmentNum = 100;
    int segmentSize = 10;
    auto networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    int requiredBufferNum = 5;
    int maxBufferAllowedToRequest = 5;
    auto localObjectBufferPool =
        std::make_shared<LocalObjectBufferPool>(networkObjectBufferPool, requiredBufferNum, maxBufferAllowedToRequest);

    std::shared_ptr<Buffer> buffer1 = localObjectBufferPool->requestBuffer();
    buffer1->RecycleBuffer();
    EXPECT_GE(localObjectBufferPool->getRequiredMemory(), static_cast<uint64_t>(segmentSize));

    std::shared_ptr<Buffer> buffer2 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer3 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer4 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer5 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer6 = localObjectBufferPool->requestBuffer();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 0);
    // Local pools use lightweight unpooled segments; the global physical-segment deque is unchanged.
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);

    buffer2->RecycleBuffer();
    buffer3->RecycleBuffer();
    buffer4->RecycleBuffer();
    buffer5->RecycleBuffer();
    buffer6->RecycleBuffer();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 5);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum);
}
