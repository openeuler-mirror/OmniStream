/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#include "TaskLocalNettyBufferMetricGroup.h"

#include <stdexcept>
#include <utility>

namespace omnistream {
    TaskLocalNettyBufferMetricGroup::TaskLocalNettyBufferMetricGroup(AbstractMetricGroup* parent,
        SizeSupplierFactory sizeSupplierFactory)
        : AbstractMetricGroup(parent), sizeSupplierFactory(std::move(sizeSupplierFactory))
    {
    }

    void TaskLocalNettyBufferMetricGroup::AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric)
    {
        metrics[metricName] = metric;
        auto sizeGauge = std::dynamic_pointer_cast<SizeGauge>(metric);
        if (sizeGauge != nullptr) {
            if (sizeSupplierFactory == nullptr) {
                throw std::runtime_error("LocalNettyBufferPool size supplier factory is null for metric: " + metricName);
            }
            sizeGauge->RegisterSupplier(sizeSupplierFactory(metricName));
        }
    }

    std::shared_ptr<Metric> TaskLocalNettyBufferMetricGroup::GetMetric(const std::string& metricName) const
    {
        auto it = metrics.find(metricName);
        if (it != metrics.end()) {
            return it->second;
        }
        return nullptr;
    }

    void TaskLocalNettyBufferMetricGroup::CleanMetrics()
    {
        metrics.clear();
    }
}
