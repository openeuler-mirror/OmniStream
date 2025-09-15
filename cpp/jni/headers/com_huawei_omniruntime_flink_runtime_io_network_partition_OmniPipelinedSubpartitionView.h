/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView
#define _Included_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView
 * Method:    createLocalChannelReader
 * Signature: (JLjava/lang/String;IJ)J
 */
JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_createLocalChannelReader
  (JNIEnv *, jobject, jlong, jstring, jint, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView
 * Method:    getNativeNextBuffer
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_getNativeNextBuffer
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView
 * Method:    doCheckIfDataAvailable
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_doCheckIfDataAvailable
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView
 * Method:    releaseNativeResource
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_partition_OmniPipelinedSubpartitionView_releaseNativeResource
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif