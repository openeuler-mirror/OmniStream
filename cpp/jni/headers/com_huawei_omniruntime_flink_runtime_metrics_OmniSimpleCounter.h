/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter
#define _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter
 * Method:    getNativeCounter
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter_getNativeCounter
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif