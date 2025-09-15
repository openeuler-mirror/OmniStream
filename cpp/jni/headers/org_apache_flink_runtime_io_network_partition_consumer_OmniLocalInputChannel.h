/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel
#define _Included_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel

#include <jni.h>
/* Header for class org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel
 * Method:    doChangeNativeLocalInputChannel
 * Signature: (JLjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel_doChangeNativeLocalInputChannel
  (JNIEnv *, jobject, jlong, jstring);

/*
 * Class:     org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel
 * Method:    sendMemorySegmentToNative
 * Signature: (JJIIIII)V
 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel_sendMemorySegmentToNative
  (JNIEnv *, jobject, jlong, jlong, jint, jint, jint, jint, jint);

/*
 * Class:     org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel
 * Method:    getRecycleBufferAddress
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_io_network_partition_consumer_OmniLocalInputChannel_getRecycleBufferAddress
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif