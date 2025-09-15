/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram
#define _Included_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram */

#ifdef __cplusplus
extern "C" {
#endif
#undef com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_serialVersionUID
#define com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_serialVersionUID 1LL
/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram
 * Method:    getNativeCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_getNativeCount
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram
 * Method:    getStatistics
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_getStatistics
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif