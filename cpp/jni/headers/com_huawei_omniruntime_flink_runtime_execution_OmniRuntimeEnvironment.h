/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 #ifndef _Included_com_huawei_omniruntime_flink_runtime_execution_OmniRuntimeEnvironment
 #define _Included_com_huawei_omniruntime_flink_runtime_execution_OmniRuntimeEnvironment

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_execution_OmniRuntimeEnvironment */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_execution_OmniRuntimeEnvironment
 * Method:    createNativeEnvironment
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_execution_OmniRuntimeEnvironment_createNativeEnvironment
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif