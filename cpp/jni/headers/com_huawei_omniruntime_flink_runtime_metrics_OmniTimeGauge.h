/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
#define _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    updateNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_updateNative
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeValue
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeValue
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeCount
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeAccumulatedCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeAccumulatedCount
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeIsMeasuring
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeIsMeasuring
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeMaxSingleMeasurement
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeMaxSingleMeasurement
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif