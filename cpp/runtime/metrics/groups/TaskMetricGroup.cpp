/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "TaskMetricGroup.h"

namespace omnistream {
    // Constructor and Destructor
    TaskMetricGroup::~TaskMetricGroup()
    {
        CleanMetrics(); // Clear all metrics when the object is destroyed
    }

    // Add methods to manage task metrics
    void TaskMetricGroup::AddTaskIOMetric(const std::string& metricName, std::shared_ptr<Metric> metric)
    {
        taskIOMetricGroup[metricName] = metric;
    }

    void TaskMetricGroup::AddInternalOperatorIOMetric(const std::string& operatorName, const std::string& metricName,
                                                      std::shared_ptr<Metric> metric)
    {
        internalOperatorIOMetricGroup[operatorName][metricName] = metric;
        operatorNames.insert(operatorName); // Add operator name to the set
    }

    std::shared_ptr<Metric> TaskMetricGroup::GetTaskIOMetric(const std::string& metricName) const
    {
        auto it = taskIOMetricGroup.find(metricName);
        if (it != taskIOMetricGroup.end()) {
            return it->second;
        }
        return nullptr;
    }

    std::shared_ptr<Metric> TaskMetricGroup::GetInternalOperatorIOMetric(
        const std::string& operatorName, const std::string& metricName) const
    {
        auto it = internalOperatorIOMetricGroup.find(operatorName);
        if (it != internalOperatorIOMetricGroup.end()) {
            auto metricIt = it->second.find(metricName);
            if (metricIt != it->second.end()) {
                return metricIt->second;
            }
        }
        return nullptr;
    }

    std::unordered_set<std::string> TaskMetricGroup::GetOperatorNames() const
    {
        return operatorNames;
    }

    void TaskMetricGroup::CleanMetrics()
    {
        taskIOMetricGroup.clear();
        internalOperatorIOMetricGroup.clear();
        operatorNames.clear();
    }
};
