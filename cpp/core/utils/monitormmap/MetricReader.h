/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

// MetricReader.h
#ifndef METRIC_READER_H
#define METRIC_READER_H

#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "common.h"
#include "MetricManager.h"
#include "SHMMetricView.h"

namespace omnistream {
class MetricReader {
public:
    static const char* sharedMemoryKeyPrefix;
    MetricReader();
    ~MetricReader();

    bool Init(const std::string& monitorKey, pid_t managerPid);
    void* GetData() const;
    size_t GetSize() const;

    void EnableMonitoring();
    void DisableMonitoring();
    bool IsEnableMonitoring() const;
    uint32_t GetNumberOfMetrics();
    SHMMetricView* GetSHMMetricView(int index);

    std::string MetricViewToString(SHMMetricView* metric);

    std::vector<SHMMetricView*> GetAllMetrics();
    void PrintAllMetrics(std::vector<SHMMetricView*>);

private:
    std::string sharedMemoryKey;
    int sharedMemoryFd;
    size_t sharedMemorySize;
    void* sharedMemoryPtr;
};
}  // namespace omnistream

#endif  // METRIC_READER_H