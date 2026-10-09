/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include "runtime/metrics/Metric.h"

namespace omnistream {
    class AbstractMetricGroup {
    public:
        explicit AbstractMetricGroup(AbstractMetricGroup* parent = nullptr) : parent(parent)
        {
        }

        virtual ~AbstractMetricGroup() = default;

        std::shared_ptr<AbstractMetricGroup> addGroup(const std::string& groupName)
        {
            auto it = groups.find(groupName);
            if (it != groups.end()) {
                return it->second;
            }

            auto group = std::make_shared<AbstractMetricGroup>(this);
            groups[groupName] = group;
            return group;
        }

        std::shared_ptr<AbstractMetricGroup> addGroup(const std::string& groupName,
                                                      std::shared_ptr<AbstractMetricGroup> group)
        {
            auto it = groups.find(groupName);
            if (it != groups.end()) {
                return it->second;
            }

            groups[groupName] = group;
            return group;
        }

        virtual void AddMetric(const std::string& metricName, std::shared_ptr<Metric> metric)
        {
            metrics[metricName] = metric;
        }
        virtual std::shared_ptr<Metric> GetMetric(const std::string& metricName) const
        {
            auto it = metrics.find(metricName);
            if (it != metrics.end())
            {
                return it->second;
            }
            return nullptr;
        }
        virtual void CleanMetrics()
        {
            metrics.clear();
        }
        AbstractMetricGroup* GetParent() const
        {
            return parent;
        }

        std::shared_ptr<AbstractMetricGroup> GetChildGroup(const std::string& groupName) const
        {
            auto it = groups.find(groupName);
            if (it != groups.end()) {
                return it->second;
            }
            return nullptr;
        }


    protected:
        std::unordered_map<std::string, std::shared_ptr<Metric>> metrics;
        std::unordered_map<std::string, std::shared_ptr<AbstractMetricGroup>> groups;
        AbstractMetricGroup* parent;
    };
}
