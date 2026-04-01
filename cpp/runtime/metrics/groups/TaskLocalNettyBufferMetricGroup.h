/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "runtime/metrics/Metric.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/groups/AbstractMetricGroup.h"

namespace omnistream {
    class TaskLocalNettyBufferMetricGroup : public AbstractMetricGroup {
    public:
        using SizeSupplierFactory = std::function<SizeGauge::SizeSupplier(const std::string&)>;

        explicit TaskLocalNettyBufferMetricGroup(AbstractMetricGroup* parent = nullptr,
            SizeSupplierFactory sizeSupplierFactory = nullptr);

        void AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric) override;
        std::shared_ptr<Metric> GetMetric(const std::string& metricName) const override;
        void CleanMetrics() override;

    private:
        SizeSupplierFactory sizeSupplierFactory;

        std::string REQUIRED_BUFFERS = "requiredBuffers";
        std::string  MAX_BUFFERS = "maxBuffers";
        std::string CURRENT_POOL_SIZE = "currentPoolSize";
        std::string AVAILABLE_BUFFERS = "availableBuffers";
        std::string  REQUESTED_BUFFERS = "requestedBuffers";
        std::string REQUEST_REGULAR_BUFFER_COUNT = "requestRegularBufferCount";
        std::string REQUEST_BIG_BUFFER_COUNT = "requestBigBufferCount";
        std::string RECYCLE_REGULAR_BUFFER_COUNT = "recycleRegularBufferCount";
        std::string RECYCLE_BIG_BUFFER_COUNT = "recycleBigBufferCount";
        std::string BIG_TOTAL_MEMORY_SIZE = "bigTotalMemorySize";
        std::string AVAILABLE_BIG_MEMORY_SIZE = "availableBigMemorySize";
    };
}
