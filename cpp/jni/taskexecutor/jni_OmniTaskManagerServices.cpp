/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 2/6/25.
//


#include <executiongraph/descriptor/TaskManagerServiceConfigurationPOD.h>
#include <nlohmann/json.hpp>

#include "com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices.h"
#include "runtime/taskexecutor/TaskManagerServices.h"
#include "common.h"

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices
 * Method:    createOmniTaskManagerServices
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices_createOmniTaskManagerServices
  (JNIEnv * jniEnv, jclass thiz, jstring taskManageServicesConfiguration)
{

    const char* taskMSCString  = jniEnv->GetStringUTFChars(taskManageServicesConfiguration, nullptr);
    std::string taskMSConfString(taskMSCString);
    jniEnv->ReleaseStringUTFChars(taskManageServicesConfiguration, taskMSCString);

    LOG("taskMSConfString " << taskMSConfString);

    nlohmann::json taskMCS = nlohmann::json::parse(taskMSConfString);
    omnistream::TaskManagerServiceConfigurationPOD taskMCSConfPOD = taskMCS;

    LOG("taskMCSConfPOD " << taskMCSConfPOD.toString());
    auto taskManagerServices = omnistream::TaskManagerServices::fromConfiguration(taskMCSConfPOD);

    return reinterpret_cast<jlong>(taskManagerServices);
}


