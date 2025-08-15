/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include <jni_md.h>
#include <jni.h>
#include "common.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge.h"
#include "com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge.h"
#include "runtime/metrics/groups/TaskMetricGroup.h"
#include "runtime/metrics/SimpleCounter.h"
#include "runtime/metrics/SizeGauge.h"
#include "runtime/metrics/TimerGauge.h"
#include "runtime/metrics/DescriptiveStatisticsHistogram.h"
using namespace omnistream;
// Updated common helper uses raw pointer for TaskMetricGroup
template <typename T>
static void registerMetric(TaskMetricGroup* taskMetricGroup, const std::string& scope, const std::string& metricNameStr,
                           std::shared_ptr<T> metric)
{
    size_t pos = scope.find('_');
    std::string firstSubstring = (pos == std::string::npos) ? scope : scope.substr(0, pos);
    if (firstSubstring == "OmniTaskIOMetricGroup") {
        taskMetricGroup->AddTaskIOMetric(metricNameStr, metric);
    } else if (firstSubstring == "OmniInternalOperatorIOMetricGroup") {
        // the operatorName is the second part of the scope
        std::string operatorName = scope.substr(pos + 33);
        taskMetricGroup->AddInternalOperatorIOMetric(operatorName, metricNameStr, metric);
    } else {
        throw std::runtime_error("Unknown metric group: " + firstSubstring);
    }
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeSimpleCounter
(JNIEnv* jniEnv, jclass, jlong taskMetricRef, jstring scope, jstring metricName)
{
    auto taskMetricGroup = reinterpret_cast<TaskMetricGroup*>(taskMetricRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto counter = std::make_shared<omnistream::SimpleCounter>();
    // Use common helper to register the counter with scope
    registerMetric(taskMetricGroup, scopeStr, metricNameStr, counter);
    return reinterpret_cast<long>(counter.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeTimeGauge
(JNIEnv* jniEnv, jclass, jlong taskMetricRef, jstring scope, jstring metricName)
{
    auto taskMetricGroup = reinterpret_cast<TaskMetricGroup*>(taskMetricRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto timerGauge = std::make_shared<omnistream::TimerGauge>();
    // Use common helper to register the timerGauge with scope
    registerMetric(taskMetricGroup, scopeStr, metricNameStr, timerGauge);
    return reinterpret_cast<long>(timerGauge.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeSizeGauge
(JNIEnv* jniEnv, jclass, jlong taskMetricRef, jstring scope, jstring metricName)
{
    auto taskMetricGroup = reinterpret_cast<TaskMetricGroup*>(taskMetricRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto sizeGauge = std::make_shared<omnistream::SizeGauge>();
    // Use common helper to register the sizeGauge with scope
    registerMetric(taskMetricGroup, scopeStr, metricNameStr, sizeGauge);
    return reinterpret_cast<long>(sizeGauge.get());
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_utils_OmniMetricHelper_createNativeOmniDescriptiveStatisticsHistogram
(JNIEnv* jniEnv, jclass, jlong taskMetricRef, jint windowSize, jstring scope, jstring metricName)
{
    auto taskMetricGroup = reinterpret_cast<TaskMetricGroup*>(taskMetricRef);
    // Convert scope parameter
    const char* scopeCStr = jniEnv->GetStringUTFChars(scope, nullptr);
    if (scopeCStr == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string for scope");
    }
    std::string scopeStr(scopeCStr);
    jniEnv->ReleaseStringUTFChars(scope, scopeCStr);
    // Convert metricName parameter
    const char* utf8String = jniEnv->GetStringUTFChars(metricName, nullptr);
    if (utf8String == nullptr) {
        throw std::runtime_error("Failed to convert jstring to std::string");
    }
    std::string metricNameStr(utf8String);
    jniEnv->ReleaseStringUTFChars(metricName, utf8String); // VERY IMPORTANT: Release the string
    auto histogram = std::make_shared<omnistream::DescriptiveStatisticsHistogram>(windowSize);
    // Use common helper to register the histogram with scope
    registerMetric(taskMetricGroup, scopeStr, metricNameStr, histogram);
    return reinterpret_cast<long>(histogram.get());
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniSimpleCounter_getNativeCounter
(JNIEnv*, jobject, jlong nativeCounter)
{
    auto counter = reinterpret_cast<SimpleCounter*>(nativeCounter);
    return counter->GetCount();
}

JNIEXPORT jint JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniSizeGauge_getNativeSize
(JNIEnv*, jobject, jlong nativeSizeGauge)
{
    auto sizeGauge = reinterpret_cast<SizeGauge*>(nativeSizeGauge);
    return sizeGauge->GetValue();
}

JNIEXPORT jlong JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_getNativeCount
(JNIEnv*, jobject, jlong nativeHistogram)
{
    auto histogram = reinterpret_cast<DescriptiveStatisticsHistogram*>(nativeHistogram);
    return histogram->GetCount();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram
 * Method:    getStatistics
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_com_huawei_omniruntime_flink_runtime_metrics_OmniDescriptiveStatisticsHistogram_getStatistics
(JNIEnv* jniEnv, jobject, jlong nativeHistogram)
{
    auto histogram = reinterpret_cast<DescriptiveStatisticsHistogram*>(nativeHistogram);
    auto statistics = histogram->GetStatistics();
    std::string statsString = statistics->ToString();
    return jniEnv->NewStringUTF(statsString.c_str());
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_updateNative
(JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    timerGauge->Update();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeValue
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeValue
(JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetValue();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeCount
(JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetCount();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeAccumulatedCount
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeAccumulatedCount
(JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetAccumulatedCount();
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeIsMeasuring
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeIsMeasuring
(JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->IsMeasuring() ? JNI_TRUE : JNI_FALSE;
}

/*
 * Class:     com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge
 * Method:    getNativeMaxSingleMeasurement
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_metrics_OmniTimeGauge_getNativeMaxSingleMeasurement
(JNIEnv*, jobject, jlong nativeTimeGauge)
{
    auto timerGauge = reinterpret_cast<TimerGauge*>(nativeTimeGauge);
    return timerGauge->GetMaxSingleMeasurement();
}
