/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 10/2/24.
//
#include "TaskPartitionerConfig.h"

namespace omnistream::datastream {
    TaskPartitionerConfig::TaskPartitionerConfig(string partitionName, int numberOfChannel, json options)
        : partitionerName(
          std::move(partitionName)), numberOfChannel(numberOfChannel), options(options) {
    }


    string TaskPartitionerConfig::getPartitionName() const
    {
        return partitionerName;
    }

    int TaskPartitionerConfig::getNumberOfChannel() const
    {
        return numberOfChannel;
    }

    json TaskPartitionerConfig::getOptions() const
    {
        return options;
    }
}