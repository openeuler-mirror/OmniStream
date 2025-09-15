/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2
#define _Included_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2 */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2
 * Method:    runStreamSource
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2_runStreamSource
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2
 * Method:    cancelStreamSource
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2_cancelStreamSource
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2
 * Method:    resetOutputBufferAndRecordWriter
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2_resetOutputBufferAndRecordWriter
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif