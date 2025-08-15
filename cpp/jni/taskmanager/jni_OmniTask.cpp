/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 1/30/25.
//
#include <stdexcept>
#include <taskmanager/OmniTask.h>
#include "common.h"

#include "com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask.h"
#include "com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher.h"
/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    setupStreamTaskBeforeInvoke
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_setupStreamTaskBeforeInvoke
  (JNIEnv * jniEnv, jobject thiz, jlong nativeTask, jstring className)
{
    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);

    const char* utf8String  = jniEnv->GetStringUTFChars(className, nullptr);
    if (utf8String == nullptr) {
          throw std::runtime_error("Failed to convert jstring to std::string");
    }

    std::string clsName(utf8String);

    jniEnv->ReleaseStringUTFChars(className, utf8String); // VERY IMPORTANT: Release the string
    LOG("class name : " << clsName)

    long streamTaskAddress = task->setupStreamTask(clsName);
    return streamTaskAddress;
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    doRunNativeTask
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunNativeTask
  (JNIEnv *, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);
    task->doRun(streamTaskAddress);
    return 1;
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunInvokeNativeTask
        (JNIEnv *, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);
    task->DoRunInvoke(streamTaskAddress);
    return 1;
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunRestoreNativeTask
        (JNIEnv *, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);
    task->DoRunRestore(streamTaskAddress);
    return 1;
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_dispatchOperatorEvent
(JNIEnv * env, jobject, jlong nativeTask, jstring operatorId, jstring eventDesc)
{
    auto *task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);

    const char *eventCharArray = (env)->GetStringUTFChars(eventDesc, nullptr);
    std::string eventString(eventCharArray);
    (env)->ReleaseStringUTFChars(eventDesc, eventCharArray);

    const char *operatorIdCharArray = (env)->GetStringUTFChars(operatorId, nullptr);
    std::string operatorIdString(operatorIdCharArray);
    (env)->ReleaseStringUTFChars(operatorId, operatorIdCharArray);

    task->dispatchOperatorEvent(operatorIdString, eventString);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_notifyRemoteDataAvailable
  (JNIEnv *, jobject, jlong nativeTask, jint inputGateIndex, jint channelIndex, jlong bufferAddress, jint bufferLength, jint sequenceNumber)
{
    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);
    task->notifyRemoteDataAvailable(inputGateIndex, channelIndex, bufferAddress, bufferLength, sequenceNumber);
}


JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_createNativeTaskMetricGroup
  (JNIEnv *, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);
    auto taskMetricGroup = task->createTaskMetricGroup();
    return reinterpret_cast<long>(taskMetricGroup.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_cancelTask
        (JNIEnv *, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);
    task->cancel();
    return reinterpret_cast<long>(0L);
}

