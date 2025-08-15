/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef TASKPARTITIONERCONFIG_H
#define TASKPARTITIONERCONFIG_H

#include <string>
#include <nlohmann/json.hpp>
using string = std::string;
using json = nlohmann::json;

namespace omnistream::datastream {
    class TaskPartitionerConfig {
    public:
        TaskPartitionerConfig(string partitionName, int numberOfChannel, nlohmann::json options);
        string getPartitionName() const;
        int getNumberOfChannel() const;
        nlohmann::json  getOptions() const;
    private:
        string partitionerName;
        int numberOfChannel;
        nlohmann::json  options;
    };
}


#endif  //TASKPARTITIONERCONFIG_H
