/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
#define _Included_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
 * Method:    createNativeTaskExecutor
 * Signature: (Ljava/lang/String;J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor_createNativeTaskExecutor
  (JNIEnv *, jobject, jstring, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor
 * Method:    submitTaskNative
 * Signature: (JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskExecutor_submitTaskNative
  (JNIEnv *, jobject, jlong, jstring, jstring, jstring);

#ifdef __cplusplus
}
#endif
#endif