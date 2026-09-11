/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */
#include "TaskBackendStateMetricGroup.h"

namespace omnistream {
    TaskBackendStateMetricGroup::TaskBackendStateMetricGroup(AbstractMetricGroup* parent)
        : AbstractMetricGroup(parent)
    {
    }

    std::shared_ptr<OperatorStateMetricGroup> TaskBackendStateMetricGroup::GetOrCreateOperatorGroup(
        const std::string& operatorName)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = operatorGroups_.find(operatorName);
        if (it != operatorGroups_.end()) {
            return it->second;
        }
        auto group = std::make_shared<OperatorStateMetricGroup>(this);
        operatorGroups_[operatorName] = group;
        addGroup(operatorName, group);
        return group;
    }

    std::vector<std::string> TaskBackendStateMetricGroup::GetOperatorGroupNames() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> names;
        names.reserve(operatorGroups_.size());
        for (const auto& pair : operatorGroups_) {
            names.push_back(pair.first);
        }
        return names;
    }
}
