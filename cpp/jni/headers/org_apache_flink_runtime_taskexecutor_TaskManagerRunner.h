/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_org_apache_flink_runtime_taskexecutor_TaskManagerRunner
#define _Included_org_apache_flink_runtime_taskexecutor_TaskManagerRunner

#include <jni.h>
/* Header for class org_apache_flink_runtime_taskexecutor_TaskManagerRunner */

#ifdef __cplusplus
extern "C" {
#endif
#undef org_apache_flink_runtime_taskexecutor_TaskManagerRunner_FAILURE_EXIT_CODE
#define org_apache_flink_runtime_taskexecutor_TaskManagerRunner_FAILURE_EXIT_CODE 1L
#undef org_apache_flink_runtime_taskexecutor_TaskManagerRunner_FATAL_ERROR_SHUTDOWN_TIMEOUT_MS
#define org_apache_flink_runtime_taskexecutor_TaskManagerRunner_FATAL_ERROR_SHUTDOWN_TIMEOUT_MS 10000LL
#undef org_apache_flink_runtime_taskexecutor_TaskManagerRunner_SUCCESS_EXIT_CODE
#define org_apache_flink_runtime_taskexecutor_TaskManagerRunner_SUCCESS_EXIT_CODE 0L
/*
 * Class:     org_apache_flink_runtime_taskexecutor_TaskManagerRunner
 * Method:    initTMConfiguration
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_taskexecutor_TaskManagerRunner_initTMConfiguration
  (JNIEnv *, jclass, jstring);

#ifdef __cplusplus
}
#endif
#endif