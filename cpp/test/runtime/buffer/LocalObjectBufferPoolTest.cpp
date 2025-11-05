#include <gtest/gtest.h>
#include "runtime/buffer/LocalObjectBufferPool.h"
#include "runtime/buffer/NetworkObjectBufferPool.h"

using namespace omnistream;
TEST(LocalObjectBufferPoolTest, RequestBuffer)
{

    int segmentNum = 100;
    int segmentSize = 10;
    std::shared_ptr<NetworkObjectBufferPool> networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    int requiredBufferNum = 5;
    int maxBufferAllowedToRequest = 5;
    std::shared_ptr<LocalObjectBufferPool> localObjectBufferPool = std::make_shared<LocalObjectBufferPool>(networkObjectBufferPool, requiredBufferNum, maxBufferAllowedToRequest);

    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 1);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum - 1);

    std::shared_ptr<Buffer> buffer1 = localObjectBufferPool->requestBuffer();
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum  - 2);
    std::shared_ptr<Buffer> buffer2 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer3 = localObjectBufferPool->requestBuffer();
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum  - 4);
    std::shared_ptr<Buffer> buffer4 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer5 = localObjectBufferPool->requestBuffer();

    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum  - 5);
    std::shared_ptr<Buffer> buffer6 = localObjectBufferPool->requestBuffer();

    EXPECT_EQ(buffer6, nullptr);

}



TEST(LocalObjectBufferPoolTest, Recycle)
{

    int segmentNum = 100;
    int segmentSize = 10;
    std::shared_ptr<NetworkObjectBufferPool> networkObjectBufferPool = std::make_shared<NetworkObjectBufferPool>(segmentNum, segmentSize);

    int requiredBufferNum = 5;
    int maxBufferAllowedToRequest = 5;
    std::shared_ptr<LocalObjectBufferPool> localObjectBufferPool = std::make_shared<LocalObjectBufferPool>(networkObjectBufferPool, requiredBufferNum, maxBufferAllowedToRequest);


    std::shared_ptr<Buffer> buffer1 = localObjectBufferPool->requestBuffer();
    buffer1->RecycleBuffer();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 2);

    std::shared_ptr<Buffer> buffer2 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer3 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer4 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer5 = localObjectBufferPool->requestBuffer();
    std::shared_ptr<Buffer> buffer6 = localObjectBufferPool->requestBuffer();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 0);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum  - 5);

    buffer2->RecycleBuffer();
    buffer3->RecycleBuffer();
    buffer4->RecycleBuffer();
    buffer5->RecycleBuffer();
    buffer6->RecycleBuffer();
    EXPECT_EQ(localObjectBufferPool->getNumberOfAvailableSegments(), 5);
    EXPECT_EQ(networkObjectBufferPool->getNumberOfAvailableObjectSegments(), segmentNum  - 5);

}