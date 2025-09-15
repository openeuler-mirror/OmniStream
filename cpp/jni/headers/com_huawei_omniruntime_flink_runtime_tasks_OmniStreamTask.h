/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask
#define _Included_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask
 * Method:    createNativeStreamTask
 * Signature: (Ljava/lang/String;JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask_createNativeStreamTask
  (JNIEnv *, jclass, jstring, jlong, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask
 * Method:    createNativeOmniInputProcessor
 * Signature: (JLjava/lang/String;I)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask_createNativeOmniInputProcessor
  (JNIEnv *, jclass, jlong, jstring, jint);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask
 * Method:    removeNativeStreamTask
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniStreamTask_removeNativeStreamTask
  (JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif