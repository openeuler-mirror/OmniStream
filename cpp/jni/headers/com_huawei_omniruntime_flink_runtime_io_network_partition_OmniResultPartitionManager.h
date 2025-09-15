/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartitionManager
#define _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartitionManager

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartitionManager */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartitionManager
 * Method:    registerResultPartitionNative
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniResultPartitionManager_registerResultPartitionNative
  (JNIEnv *, jobject, jlong, jlong);

#ifdef __cplusplus
}
#endif
#endif