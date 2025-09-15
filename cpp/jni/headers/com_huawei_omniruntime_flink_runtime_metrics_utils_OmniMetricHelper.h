/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper
#define _Included_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper
 * Method:    createNativeSimpleCounter
 * Signature: (JLjava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeSimpleCounter
  (JNIEnv *, jclass, jlong, jstring, jstring);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper
 * Method:    createNativeTimeGauge
 * Signature: (JLjava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeTimeGauge
  (JNIEnv *, jclass, jlong, jstring, jstring);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper
 * Method:    createNativeSizeGauge
 * Signature: (JLjava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeSizeGauge
  (JNIEnv *, jclass, jlong, jstring, jstring);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper
 * Method:    createNativeOmniDescriptiveStatisticsHistogram
 * Signature: (JILjava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeOmniDescriptiveStatisticsHistogram
  (JNIEnv *, jclass, jlong, jint, jstring, jstring);

#ifdef __cplusplus
}
#endif
#endif