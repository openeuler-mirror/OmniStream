/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef NETTY_BUFFER_POOL_H
#define NETTY_BUFFER_POOL_H
#include <cstdint>
#include <memory>  // added for shared_ptr
#include <mutex>   // added for thread-safety
#include <queue>
#include <unordered_map>
#include <set>
#include "NettyBufferInfo.h"

namespace omnistream {
    class NettyBufferPool {
    public:
        NettyBufferPool(int poolSize, int bufferSize);
        ~NettyBufferPool();
        // Updated to return shared_ptr
        std::shared_ptr<NettyBufferInfo> RequestBuffer();
        std::shared_ptr<NettyBufferInfo> RequestBigBuffer(int size);
        void RecycleBuffer(long bufferAddress);
        void DestroyCorruptBuffer(long bufferAddress);
        void AddBuffer();
        void PrintAllBufferAddresses();
        bool ContainsBuffer(std::shared_ptr<NettyBufferInfo> bufferInfo) ;
        bool RemoveBuffer(std::shared_ptr<NettyBufferInfo> bufferInfo) ;
    private:
        int poolSize_;
        int bufferSize_;
        // Updated to use shared_ptr instead of a raw type
        std::queue<std::shared_ptr<NettyBufferInfo>> bufferPool_;
        std::unordered_map<long, std::shared_ptr<NettyBufferInfo>> allBuffers_; // changed to store shared_ptr
        std::recursive_mutex nettyBufferPoolMtx_; // added mutex for thread-safety
    };
} // namespace omnistream
#endif  // NETTY_BUFFER_POOL_H
