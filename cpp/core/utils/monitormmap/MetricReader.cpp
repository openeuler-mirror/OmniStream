/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
// MetricReader.cpp
#include "MetricReader.h"
#include <sstream>

namespace omnistream {
    const char* MetricReader::sharedMemoryKeyPrefix = "OMNI_SHM_METRIC";

    MetricReader::MetricReader() : sharedMemoryFd(-1),
        sharedMemorySize(0),
        sharedMemoryPtr(nullptr) {}

    MetricReader::~MetricReader()
    {
        if (sharedMemoryPtr != nullptr) {
            if (munmap(sharedMemoryPtr, sharedMemorySize) == -1) {
                std::stringstream ss_;
                ss_ << "munmap failed for key: " << sharedMemoryKey;
                GErrorLog(ss_.str());
            }
            sharedMemoryPtr = nullptr;
        }
        if (sharedMemoryFd != -1) {
            close(sharedMemoryFd);
            sharedMemoryFd = -1;
        }
    }

    bool MetricReader::Init(const std::string& monitorKey, pid_t managerPid)
    {
        if (sharedMemoryPtr != nullptr) {
            LOG("Already initialized for key: " << sharedMemoryKey);
            return true;
        }

        std::stringstream ss_;
        ss_ << monitorKey << "_" << managerPid;
        sharedMemoryKey = ss_.str();

        sharedMemoryFd = shm_open(sharedMemoryKey.c_str(), O_RDWR, 0);
        if (sharedMemoryFd == -1) {
            std::stringstream ss_;
            ss_ << "shm_open failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
            return false;
        }

        struct stat shmStat;
        if (fstat(sharedMemoryFd, &shmStat) == -1) {
            std::stringstream ss_;
            ss_ << "fstat failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
            close(sharedMemoryFd);
            sharedMemoryFd = -1;
            return false;
        }
        sharedMemorySize = shmStat.st_size;

        sharedMemoryPtr = mmap(nullptr, sharedMemorySize, PROT_READ | PROT_WRITE, MAP_SHARED, sharedMemoryFd, 0);
        if (sharedMemoryPtr == MAP_FAILED) {
            std::stringstream ss_;
            ss_ << "mmap failed for key: " << sharedMemoryKey;
            GErrorLog(ss_.str());
            close(sharedMemoryFd);
            sharedMemoryFd = -1;
            sharedMemorySize = 0;
            sharedMemoryPtr = nullptr;
            return false;
        }

        LOG("Successfully hooked to shared memory for key: " << sharedMemoryKey << " with size: " << sharedMemorySize);
        return true;
    }

    void* MetricReader::GetData() const
    {
        return sharedMemoryPtr;
    }

    size_t MetricReader::GetSize() const
    {
        return sharedMemorySize;
    }

    void MetricReader::EnableMonitoring()
    {
        *static_cast<uint8_t*>(sharedMemoryPtr) = 1;
    }

    void MetricReader::DisableMonitoring()
    {
        *static_cast<uint8_t*>(sharedMemoryPtr) = 0;
    }


    bool MetricReader::IsEnableMonitoring() const
    {
        return (*static_cast<uint8_t*>(sharedMemoryPtr) != 0);
    }

    uint32_t MetricReader::GetNumberOfMetrics()
    {
        uint32_t* numberPtr = (static_cast<uint32_t*>(sharedMemoryPtr)) + 1;
        return *numberPtr;
    }

    SHMMetricView* MetricReader::GetSHMMetricView(int index)
    {
        auto valuePtr = static_cast<long int*>(sharedMemoryPtr) + 1 + index * 4;
        return new SHMMetricView(valuePtr);
    }

    std::string MetricReader::MetricViewToString(SHMMetricView* metric)
    {
        std::stringstream ss_;
        ss_ << "[" << sharedMemoryKey << "] " << "ThreadID: " << metric->GetThreadID()
           << " ProbeID: " << metric->GetProbeID()  << " Milliseconds: " << metric->GetMilliSeconds()
            << " Counter: " << metric->GetCount();
        return ss_.str();
    }

    std::vector<SHMMetricView*> MetricReader::GetAllMetrics()
    {
        uint32_t number = GetNumberOfMetrics();
        std::vector<SHMMetricView*> allMetrics;
        for (size_t i = 0; i < number; i++) {
            SHMMetricView* metric = GetSHMMetricView(i);
            allMetrics.push_back(metric);
        }
        return allMetrics;
    }

    void MetricReader::PrintAllMetrics(std::vector<SHMMetricView*> allMetrics)
    {
        for (SHMMetricView* metric: allMetrics) {
            std::string metricString = MetricViewToString(metric);
            std::cout << metricString << std::endl;
        }
    }
} // namespace omnistream
