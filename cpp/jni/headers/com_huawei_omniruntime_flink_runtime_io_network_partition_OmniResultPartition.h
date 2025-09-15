/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartition
#define _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartition

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartition */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartition
 * Method:    getBufferPoolAddress
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartition_getBufferPoolAddress
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartition
 * Method:    setupNative
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartition_setupNative
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif