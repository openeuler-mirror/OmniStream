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

#include <stdexcept>
#include <thread>
#include <taskmanager/OmniTask.h>
#include "common.h"
#include <bridge/OmniTaskBridgeImpl2.h>
#include "com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask.h"
#include "com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher.h"
#include "checkpoint/SavepointType.h"
#include <bridge/RemoteDataFetcherBridgeImpl.h>

namespace {
// Keep the split restore/invoke status contract aligned with origin/2026_930_poc:
// Java treats zero as success and any non-zero value as failure.
constexpr jlong SPLIT_RUN_STATUS_SUCCESS = 0;
constexpr jlong SPLIT_RUN_STATUS_FAILURE = 1;

jlong ThrowJavaRuntimeException(JNIEnv* env, const std::string& message, jlong returnCode = 0)
{
    ERROR_RELEASE(message);
    if (env->ExceptionCheck()) {
        ERROR_RELEASE("Java exception is already pending; preserve the original exception");
        return returnCode;
    }

    jclass exceptionClass = env->FindClass("java/lang/RuntimeException");
    if (exceptionClass == nullptr) {
        ERROR_RELEASE("Failed to resolve java/lang/RuntimeException");
        return returnCode;
    }

    env->ThrowNew(exceptionClass, message.c_str());
    env->DeleteLocalRef(exceptionClass);
    return returnCode;
}
} // namespace

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    setupStreamTaskBeforeInvoke
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_setupStreamTaskBeforeInvoke(
    JNIEnv* jniEnv, jobject thiz, jlong nativeTask, jstring className)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    if (task == nullptr) {
        return ThrowJavaRuntimeException(jniEnv, "setupStreamTaskBeforeInvoke received a null native task");
    }
    if (className == nullptr) {
        return ThrowJavaRuntimeException(jniEnv, "setupStreamTaskBeforeInvoke received a null stream task class");
    }

    const char* utf8String = jniEnv->GetStringUTFChars(className, nullptr);
    if (utf8String == nullptr) {
        return ThrowJavaRuntimeException(
            jniEnv, "setupStreamTaskBeforeInvoke failed to convert stream task class name");
    }

    std::string clsName(utf8String);

    jniEnv->ReleaseStringUTFChars(className, utf8String); // VERY IMPORTANT: Release the string
    LOG("class name : " << clsName);

    try {
        long streamTaskAddress = task->setupStreamTask(clsName);
        if (streamTaskAddress == 0) {
            return ThrowJavaRuntimeException(jniEnv, "native stream task setup returned a zero address");
        }
        return streamTaskAddress;
    } catch (const std::exception& e) {
        return ThrowJavaRuntimeException(jniEnv, std::string("native stream task setup failed: ") + e.what());
    } catch (...) {
        return ThrowJavaRuntimeException(jniEnv, "native stream task setup failed with unknown exception");
    }
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    doRunNativeTask
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunNativeTask(
    JNIEnv* env, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    try {
        if (task == nullptr) {
            return ThrowJavaRuntimeException(env, "doRunNativeTask received a null native task");
        }
        task->doRun(streamTaskAddress);
        return 1;
    } catch (const std::exception& e) {
        return ThrowJavaRuntimeException(env, std::string("native stream task run failed: ") + e.what());
    } catch (...) {
        return ThrowJavaRuntimeException(env, "native stream task run failed with unknown exception");
    }
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunInvokeNativeTask(
    JNIEnv* env, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    try {
        if (task == nullptr) {
            return ThrowJavaRuntimeException(
                env, "doRunInvokeNativeTask received a null native task", SPLIT_RUN_STATUS_FAILURE);
        }
        task->DoRunInvoke(streamTaskAddress);
        return SPLIT_RUN_STATUS_SUCCESS;
    } catch (const std::exception& e) {
        return ThrowJavaRuntimeException(
            env, std::string("native stream task invoke failed: ") + e.what(), SPLIT_RUN_STATUS_FAILURE);
    } catch (...) {
        return ThrowJavaRuntimeException(
            env, "native stream task invoke failed with unknown exception", SPLIT_RUN_STATUS_FAILURE);
    }
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunRestoreNativeTask(
    JNIEnv* env, jobject, jlong nativeTask, jlong streamTaskAddress)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    try {
        if (task == nullptr) {
            return ThrowJavaRuntimeException(
                env, "doRunRestoreNativeTask received a null native task", SPLIT_RUN_STATUS_FAILURE);
        }
        task->DoRunRestore(streamTaskAddress);
        return SPLIT_RUN_STATUS_SUCCESS;
    } catch (const std::exception& e) {
        return ThrowJavaRuntimeException(
            env, std::string("native stream task restore failed: ") + e.what(), SPLIT_RUN_STATUS_FAILURE);
    } catch (...) {
        return ThrowJavaRuntimeException(
            env, "native stream task restore failed with unknown exception", SPLIT_RUN_STATUS_FAILURE);
    }
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doDeleteNativeTask(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    delete task;
    return 1;
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_dispatchOperatorEvent(
    JNIEnv* env, jobject, jlong nativeTask, jstring operatorId, jstring eventDesc)
{
    auto* task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);

    const char* eventCharArray = (env)->GetStringUTFChars(eventDesc, nullptr);
    std::string eventString(eventCharArray);
    (env)->ReleaseStringUTFChars(eventDesc, eventCharArray);

    const char* operatorIdCharArray = (env)->GetStringUTFChars(operatorId, nullptr);
    std::string operatorIdString(operatorIdCharArray);
    (env)->ReleaseStringUTFChars(operatorId, operatorIdCharArray);

    task->dispatchOperatorEvent(operatorIdString, eventString);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_notifyChannelToOmni(
    JNIEnv* env, jobject, jlong nativeTaskRef, jstring partitionIdJson)
{
    const char* paritionIdChars = (env)->GetStringUTFChars(partitionIdJson, nullptr);
    std::string paritionIdStr(paritionIdChars);
    (env)->ReleaseStringUTFChars(partitionIdJson, paritionIdChars);

    nlohmann::json partitionId = nlohmann::json::parse(paritionIdStr);
    omnistream::ResultPartitionIDPOD partitionIdPOD = partitionId;
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTaskRef);
    task->notifyChannelToOmni(partitionIdPOD);
}

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_notifyRemoteDataAvailable(
    JNIEnv*,
    jobject,
    jlong nativeTask,
    jint inputGateIndex,
    jint channelIndex,
    jlong bufferAddress,
    jint bufferLength,
    jint readIndex,
    jint sequenceNumber,
    jboolean isBuffer,
    jint bufferType)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyRemoteDataAvailable(
        inputGateIndex, channelIndex, bufferAddress, bufferLength, readIndex, sequenceNumber, isBuffer, bufferType);
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_createNativeTaskMetricGroup(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    auto taskMetricGroup = task->createTaskMetricGroup();
    return reinterpret_cast<long>(taskMetricGroup.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_cancelTask(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->cancel();
    return reinterpret_cast<long>(0L);
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_getRecycleBufferAddress(
    JNIEnv*, jobject, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    return task->GetRecycleBufferAddress();
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_triggerCheckpointCpp(
    JNIEnv* jniEnv,
    jobject,
    jlong nativeTask,
    jlong checkpointID,
    jlong checkpointTimestamp,
    jstring checkpointoptionJson)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    if (!task) {
        ERROR_RELEASE("OmniTask_triggerCheckpointCpp task is null");
        THROW_LOGIC_EXCEPTION("OmniTask_triggerCheckpointCpp task is null");
    }
    const char* checkpointStr = jniEnv->GetStringUTFChars(checkpointoptionJson, nullptr);
    nlohmann::json checkpointoptionJsonStr = json::parse(checkpointStr);
    jniEnv->ReleaseStringUTFChars(checkpointoptionJson, checkpointStr);
    std::shared_ptr<CheckpointOptions> configuredOptions(CheckpointOptions::FromJson(checkpointoptionJsonStr));
    task->triggerCheckpointBarrier(checkpointID, checkpointTimestamp, configuredOptions);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_abortCpp(
    JNIEnv*, jobject, jlong nativeTask, jlong checkpointId, jlong latestCompletedCheckpointId)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyCheckpointAborted(checkpointId, latestCompletedCheckpointId);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_completeCpp(
    JNIEnv*, jobject, jlong nativeTask, jlong checkpointId, jlong inputState)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyCheckpointComplete(checkpointId, inputState);
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    subsumedCpp
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_subsumedCpp(
    JNIEnv*, jobject, jlong nativeTask, jlong latestCompletedCheckpointId)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    task->notifyCheckpointSubsumed(latestCompletedCheckpointId);
}

JNIEXPORT void JNICALL
Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_registerRemoteDataFetcherToNative(
    JNIEnv* env, jobject thiz, jlong nativeTask)
{
    auto task = reinterpret_cast<omnistream::OmniTask*>(nativeTask);
    auto bridgeBase = task->GetRemoteDataFetcherBridge();
    if (!bridgeBase) {
        LOG("GetRemoteDataFetcherBridge returned null");
        return;
    }
    std::shared_ptr<RemoteDataFetcherBridgeImpl> remote =
        std::static_pointer_cast<RemoteDataFetcherBridgeImpl>(bridgeBase);
    remote->SetJavaRemoteDataFetcher(env, thiz);
}
