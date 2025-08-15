/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#pragma once
#include <unordered_map>
#include <string>
#include <memory>
#include <unordered_set>
#include "runtime/metrics/Metric.h"

namespace omnistream {
    class TaskMetricGroup {
    public:
        TaskMetricGroup() = default;
        // Clear all metrics
        ~TaskMetricGroup();
        void CleanMetrics();
        // Add methods to manage task metrics
        void AddTaskIOMetric(const std::string& metricName, std::shared_ptr<Metric> metric);
        void AddInternalOperatorIOMetric(const std::string& scope, const std::string& metricName,
                                         std::shared_ptr<Metric> metric);
        std::shared_ptr<Metric> GetTaskIOMetric(const std::string& metricName) const;
        std::shared_ptr<Metric> GetInternalOperatorIOMetric(const std::string& operatorName,
                                                            const std::string& metricName) const;
        // get all operator names
        std::unordered_set<std::string> GetOperatorNames() const;

    private:
        // task io metric group
        std::unordered_map<std::string, std::shared_ptr<Metric>> taskIOMetricGroup;
        // internal operator io metric group
        std::unordered_map<std::string, std::unordered_map<std::string, std::shared_ptr<Metric>>>
        internalOperatorIOMetricGroup;
        // store all operatorNames in a set structure
        std::unordered_set<std::string> operatorNames;
        // you can add the extended metric group here
    };
}
