/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/29/25.
//

#include <jni.h>

#include <nlohmann/json.hpp>
#include "com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor.h"
#include "runtime/taskexecutor/OmniTaskExecutor.h"
#include "common.h"
#include "runtime/executiongraph/JobInformationPOD.h"
#include "runtime/executiongraph/TaskInformationPOD.h"
#include "runtime/executiongraph/descriptor/TaskDeploymentDescriptorPOD.h"

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
 * Method:    createNativeTaskExecutor
 * Signature: (Ljava/lang/String;J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor_createNativeTaskExecutor
  (JNIEnv *jnienv, jobject thiz, jstring taskExecutorConfiguration, jlong nativeTaskManagerServiceAddress)
{
    auto taskManagerServices = reinterpret_cast<omnistream::TaskManagerServices *>(nativeTaskManagerServiceAddress);
    auto taskManagerServicesWrap = std::shared_ptr<omnistream::TaskManagerServices>(taskManagerServices);
    omnistream::OmniTaskExecutor* omniTaskExecutor = new omnistream::OmniTaskExecutor(taskManagerServicesWrap);
    return reinterpret_cast<jlong>(omniTaskExecutor);
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
 * Method:    submitTaskNative
 * Signature: (JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor_submitTaskNative
  (JNIEnv * jniEnv, jobject thiz, jlong nativeTaskExecutorAddress, jstring jobjson, jstring taskjson, jstring tddjson)
{
    const char* jobString  = jniEnv->GetStringUTFChars(jobjson, nullptr);
    std::string jobInfoString(jobString);
    jniEnv->ReleaseStringUTFChars(jobjson, jobString);

    const char* taskString  = jniEnv->GetStringUTFChars(taskjson, nullptr);
    std::string taskInfoString(taskString);
    jniEnv->ReleaseStringUTFChars(taskjson, taskString);

    const char* tddString  = jniEnv->GetStringUTFChars(tddjson, nullptr);
    std::string taskDDString(tddString);
    jniEnv->ReleaseStringUTFChars(tddjson, tddString);


    LOG("jobjson " << jobInfoString);
    LOG("taskjson " << taskInfoString);
    LOG("tddjson " << taskDDString);

    nlohmann::json job = nlohmann::json::parse(jobInfoString);
    omnistream::JobInformationPOD jobInfo = job;
    nlohmann::json task = nlohmann::json::parse(taskInfoString);
    omnistream::TaskInformationPOD taskInfo = task;
    nlohmann::json tdd = nlohmann::json::parse(taskDDString);
    omnistream::TaskDeploymentDescriptorPOD tddinfo = tdd;

    auto omniTaskExecutor = reinterpret_cast<omnistream::OmniTaskExecutor *> (nativeTaskExecutorAddress);
    auto nativeTask  = omniTaskExecutor ->submitTask(jobInfo, taskInfo, tddinfo);
    return reinterpret_cast<jlong>(nativeTask);
}