/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2
#define _Included_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2 */

#ifdef __cplusplus
extern "C" {
#endif
/*
 • Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2

 • Method:    getNativeSourceOperator

 • Signature: (J)J

 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2_getNativeSourceOperator
  (JNIEnv *, jclass, jlong);

/*
 • Class:     com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2

 • Method:    handleOperatorEvent

 • Signature: (JLjava/lang/String;)V

 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2_handleOperatorEvent
  (JNIEnv *, jclass, jlong, jstring);

#ifdef __cplusplus
}
#endif
#endif