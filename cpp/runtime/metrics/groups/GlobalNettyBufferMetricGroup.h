/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#pragma once

#include <functional>
#include <memory>
#include <string>

#include "runtime/metrics/Metric.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/groups/AbstractMetricGroup.h"

namespace omnistream {
    class GlobalNettyBufferMetricGroup : public AbstractMetricGroup {
    public:
        using SizeSupplierFactory = std::function<SizeGauge::SizeSupplier(const std::string&)>;

        explicit GlobalNettyBufferMetricGroup(AbstractMetricGroup* parent = nullptr,
            SizeSupplierFactory sizeSupplierFactory = nullptr);

        void SetSizeSupplierFactory(SizeSupplierFactory sizeSupplierFactory);
        void AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric) override;

    private:
        SizeSupplierFactory sizeSupplierFactory;

        std::string TOTAL_NUMBER_OF_BUFFERS = "totalNumberOfBuffers";
        std::string ALLOCATED_REGULAR_BUFFER_COUNT = "allocatedRegularBufferCount";
        std::string NUM_TOTAL_REQUIRED_BUFFERS = "numTotalRequiredBuffers";
        std::string ALL_LOCAL_POOLS_SIZE = "allLocalPoolsSize";
        std::string AVAILABLE_BUFFERS = "availableBuffers";
    };
}
