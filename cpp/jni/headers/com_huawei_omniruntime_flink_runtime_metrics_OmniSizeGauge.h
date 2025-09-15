/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge
#define _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge
 * Method:    getNativeSize
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge_getNativeSize
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif