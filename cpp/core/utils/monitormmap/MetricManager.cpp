/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// metric_manager.cpp
#include "MetricManager.h"
#include <sstream>
#include <cstring>

namespace omnistream {

    const char* MetricManager::sharedMemoryKeyPrefix = "OMNI_SHM_METRIC";
    long MetricManager::omniStreamTaskProcessInputID = 1;
    std::unique_ptr<MetricManager> MetricManager::instance;
    std::mutex MetricManager::singletonMutex;
    const int MetricManager::sharedMemoryFDMode = 0666;

MetricManager::MetricManager(const std::string& monitorKey) : monitorKey(monitorKey) {}

MetricManager::~MetricManager()
{
    if (sharedMemoryPtr != nullptr) {
        if (munmap(sharedMemoryPtr, sharedMemorySize) == -1) {
            std::stringstream ss_;
            ss_ << "munmap failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
        }
    }
    if (sharedMemoryFd != -1) {
        if (close(sharedMemoryFd) == -1) {
            std::stringstream ss_;
            ss_ << "close failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
        }
        if (shm_unlink(sharedMemoryKey.c_str()) == -1) {
            std::stringstream ss_;
            ss_ << "shm_unlink failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
        }
    }
}

bool MetricManager::Setup(size_t size)
{
    pid_t pid = getpid();
    std::stringstream ss_;
    ss_ << "/" << monitorKey << "_" << pid;
    sharedMemoryKey = ss_.str();
    sharedMemorySize = size;

    // Check if shared memory object already exists
    sharedMemoryFd = shm_open(sharedMemoryKey.c_str(), O_RDWR, sharedMemoryFDMode);
    if (sharedMemoryFd == -1) {
        if (errno == ENOENT) {
            // Shared memory object does not exist, create it
            return CreateSharedMemory(size);
        } else {
            std::stringstream ss_;
            ss_ << "shm_open failed for key: " << sharedMemoryKey << ", error: " << std::strerror(errno);
            GErrorLog(ss_.str());
            return false;
        }
    } else {
        // Shared memory object exists, map it
        sharedMemoryPtr = mmap(nullptr, sharedMemorySize, PROT_READ | PROT_WRITE, MAP_SHARED, sharedMemoryFd, 0);
        if (sharedMemoryPtr == MAP_FAILED) {
            std::stringstream ss_;
            ss_ << "mmap failed for existing key: " << sharedMemoryKey << ", error: " << strerror(errno);
            GErrorLog(ss_.str());
            close(sharedMemoryFd);
            sharedMemoryFd = -1;
            return false;
        }

        DisableMonitoring();
        INFO_RELEASE("Successfully hooked to existing shared memory: " << sharedMemoryKey);
        return true;
    }
}

void* MetricManager::GetDataPtr() const
{
    return sharedMemoryPtr;
}

size_t MetricManager::GetSize() const
{
    return sharedMemorySize;
}

void MetricManager::EnableMonitoring()
{
    *static_cast<uint8_t*>(sharedMemoryPtr) = 1;
}

void MetricManager::DisableMonitoring()
{
    *static_cast<uint8_t*>(sharedMemoryPtr) = 1;
}

bool MetricManager::CreateSharedMemory(size_t size)
{
    sharedMemoryFd = shm_open(sharedMemoryKey.c_str(), O_CREAT | O_RDWR, sharedMemoryFDMode);
    if (sharedMemoryFd == -1) {
        std::stringstream ss_;
        ss_ << "shm_open (creation) failed for key: " << sharedMemoryKey << ", error: " << strerror(errno);
        GErrorLog(ss_.str());
        return false;
    }

    if (ftruncate(sharedMemoryFd, size) == -1) {
        std::stringstream ss_;
        ss_ << "ftruncate failed for key: " << sharedMemoryKey << ", error: " << strerror(errno);
        GErrorLog(ss_.str());
        close(sharedMemoryFd);
        shm_unlink(sharedMemoryKey.c_str());
        sharedMemoryFd = -1;
        return false;
    }

    sharedMemoryPtr = mmap(nullptr, sharedMemorySize, PROT_READ | PROT_WRITE, MAP_SHARED, sharedMemoryFd, 0);
    if (sharedMemoryPtr == MAP_FAILED) {
        std::stringstream ss_;
        ss_ << "mmap failed for newly created key: " << sharedMemoryKey << ", error: " << strerror(errno);
        GErrorLog(ss_.str());
        close(sharedMemoryFd);
        shm_unlink(sharedMemoryKey.c_str());
        sharedMemoryFd = -1;
        return false;
    }

    LOG("Successfully created and mapped shared memory: " << sharedMemoryKey << " with size: " << sharedMemorySize);
    return true;
}

} // namespace xost::share_mem_monitor