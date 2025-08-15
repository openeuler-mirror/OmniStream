/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "NettyBufferPool.h"
using namespace std;

namespace omnistream {
    NettyBufferPool::NettyBufferPool(int poolSize, int bufferSize)
        : poolSize_(poolSize), bufferSize_(bufferSize)
    {
        for (int i = 0; i < poolSize_; ++i) {
            uint8_t* buffer = new uint8_t[bufferSize_]{};
            auto info_ptr = std::make_shared<NettyBufferInfo>(buffer, bufferSize_);
            bufferPool_.push(info_ptr);
            allBuffers_.insert({reinterpret_cast<long>(buffer), info_ptr});
        }
    }

    NettyBufferPool::~NettyBufferPool()
    {
    }

    // Returns an available buffer of the required size.
    // If requested size is greater than pool's bufferSize_,
    // a new buffer is allocated, stored in allBuffers_, and returned.
    std::shared_ptr<NettyBufferInfo> NettyBufferPool::RequestBuffer()
    {
        LOG("request regular buffer from NettyBufferPool  remain regular pool size::" << bufferPool_.size())
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_); // added for concurrency
        if (bufferPool_.empty()) {
            return nullptr; // No available buffer
        }
        auto info_ptr = bufferPool_.front();
        bufferPool_.pop();
        return info_ptr; // return shared_ptr directly
    }

    std::shared_ptr<NettyBufferInfo> NettyBufferPool::RequestBigBuffer(int batchSize)
    {
        int newSize = batchSize + NettyBufferInfo::elementNumBytes;
        LOG("request big size buffer from NettyBufferPool:: " << batchSize
            << " remain regular pool size::" << bufferPool_.size())
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_); // added for concurrency
        uint8_t* newBuffer = new uint8_t[newSize]{};
        auto info_ptr = std::make_shared<NettyBufferInfo>(newBuffer, newSize);
        allBuffers_.insert({reinterpret_cast<long>(newBuffer), info_ptr});
        return info_ptr; // return shared_ptr directly
    }

    // Recycles a buffer back into the pool after verifying it belongs to this
    // pool. Accepts a long type address.
    void NettyBufferPool::RecycleBuffer(long bufferAddress)
    {
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_); // added for concurrency
        auto it = allBuffers_.find(bufferAddress);
        if (it == allBuffers_.end()) {
            return;
        }

        if (it->second->GetSize() > bufferSize_) {
            allBuffers_.erase(it);
            return;
        }

        it->second->ResetBuffer();
        bufferPool_.push(it->second);
    }
    void NettyBufferPool::DestroyCorruptBuffer(long bufferAddress)
    {
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_); // added for concurrency
        auto it = allBuffers_.find(bufferAddress);
        if (it != allBuffers_.end()) {
            allBuffers_.erase(it);
        }
    }

    void NettyBufferPool::AddBuffer()
    {
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_); // added for concurrency
        if (bufferPool_.size() < static_cast<size_t>(poolSize_)) {
            try {
                uint8_t* buffer = new uint8_t[bufferSize_];
                auto info_ptr = std::make_shared<NettyBufferInfo>(buffer, bufferSize_);
                bufferPool_.push(info_ptr);
                allBuffers_.insert({reinterpret_cast<long>(buffer), info_ptr});
                PrintAllBufferAddresses();
            } catch (...) {
                INFO_RELEASE("???????? NEW INI[]  ERROR")
                throw;
            }
        }
    }

    void NettyBufferPool::PrintAllBufferAddresses()
    {
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_);
        INFO_RELEASE("Buffer addresses in bufferPool_:");
        std::queue<std::shared_ptr<NettyBufferInfo>> tempQueue = bufferPool_;
        while (!tempQueue.empty()) {
            auto info = tempQueue.front();
            tempQueue.pop();
            INFO_RELEASE(
                "^^^^^^^^^^^^^^^^^^^^^  bufferPool_ address = " << reinterpret_cast<long>(info->GetOriginalAddress()));
        }
        INFO_RELEASE("Buffer addresses in allBuffers_:");
        for (const auto& pair : allBuffers_) {
            INFO_RELEASE(
                "^^^^^^^^^^^^^^^^^^^^^^  allBuffers_ key = " << pair.first << ", address = " << reinterpret_cast<long>(
                    pair.second->GetAddress()));
        }
    }

    bool NettyBufferPool::ContainsBuffer(std::shared_ptr<NettyBufferInfo> bufferInfo)
    {
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_);
        std::queue<std::shared_ptr<NettyBufferInfo>> tempQueue = bufferPool_;
        while (!tempQueue.empty()) {
            if (tempQueue.front() == bufferInfo) {
                return true;
            }
            tempQueue.pop();
        }
        return false;
    }

    bool NettyBufferPool::RemoveBuffer(std::shared_ptr<NettyBufferInfo> bufferInfo)
    {
        std::lock_guard<std::recursive_mutex> lock(nettyBufferPoolMtx_);
        std::queue<std::shared_ptr<NettyBufferInfo>> newQueue;
        bool found = false;
        while (!bufferPool_.empty()) {
            auto current = bufferPool_.front();
            bufferPool_.pop();
            if (current == bufferInfo) {
                found = true;
                continue;
            }
            newQueue.push(current);
        }
        bufferPool_ = std::move(newQueue);
        return found;
    }

} // namespace omnistream
