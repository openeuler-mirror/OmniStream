/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>


#include "core/api/common/TaskInfoImpl.h"
#include "../include/common.h"
#include "runtime/taskmanager/RuntimeEnvironment.h"

#include "../core/io/StreamOneInputProcessor.h"
#include "../core/task/StreamTask.h"
#include "core/utils/monitormmap/MetricManager.h"

#include "com_huawei_omniruntime_flink_TNELLibrary.h"
#include "com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput.h"

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask.h"

// for convenience
using json = nlohmann::json;

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_TNELLibrary_initialize(JNIEnv *, jclass)
{
    omnistream::MetricManager* metric_manager = omnistream::MetricManager::GetInstance();
    bool result  = metric_manager->Setup(32768);
    if (result)
    {
        INFO_RELEASE("Shared Memory Metric Manager Loading Succeed!")
    } else
    {
        INFO_RELEASE("Shared Memory Metric Manager Loading Failed!")
    }
}
