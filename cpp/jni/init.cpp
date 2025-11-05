/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>


#include "core/api/common/TaskInfoImpl.h"
#include "../include/common.h"

#include "../streaming/runtime/io/StreamOneInputProcessor.h"
#include "../streaming/runtime/tasks/StreamTask.h"
#include "core/utils/monitormmap/MetricManager.h"

#include "com_huawei_omniruntime_flink_TNELLibrary.h"
#include "com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput.h"

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask.h"
#include "org_apache_flink_runtime_taskexecutor_TaskManagerRunner.h"
#include "include/functions/Configuration.h"

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

JNIEXPORT void JNICALL Java_org_apache_flink_runtime_taskexecutor_TaskManagerRunner_initTMConfiguration(JNIEnv *env, jclass, jstring configStr)
{
    const char *cStrCon = (env)->GetStringUTFChars(configStr, 0);
    nlohmann::json config = nlohmann::json::parse(cStrCon);
    Configuration::TM_CONFIG->setConfiguration(config);
}
