/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher
#define _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher
 * Method:    notifyRemoteDataAvailable
 * Signature: (JIIJII)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_RemoteDataFetcher_notifyRemoteDataAvailable
  (JNIEnv *, jobject, jlong, jint, jint, jlong, jint, jint);

#ifdef __cplusplus
}
#endif
#endif