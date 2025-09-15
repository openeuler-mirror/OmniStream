/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices
#define _Included_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices
 * Method:    createOmniTaskManagerServices
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_taskexecutor_OmniTaskManagerServices_createOmniTaskManagerServices
  (JNIEnv *, jclass, jstring);

#ifdef __cplusplus
}
#endif
#endif