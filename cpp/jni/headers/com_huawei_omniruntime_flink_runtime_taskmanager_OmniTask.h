/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
#define _Included_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    setupStreamTaskBeforeInvoke
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_setupStreamTaskBeforeInvoke
  (JNIEnv *, jobject, jlong, jstring);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    doRunRestoreNativeTask
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunRestoreNativeTask
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    doRunInvokeNativeTask
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunInvokeNativeTask
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    dispatchOperatorEvent
 * Signature: (JLjava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_dispatchOperatorEvent
  (JNIEnv *, jobject, jlong, jstring, jstring);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    doRunNativeTask
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_doRunNativeTask
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    createNativeTaskMetricGroup
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_createNativeTaskMetricGroup
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask
 * Method:    cancelTask
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskmanager_OmniTask_cancelTask
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif