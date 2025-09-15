/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader
#define _Included_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader

#include <jni.h>
/* Header for class org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader */

#ifdef __cplusplus
extern "C" {
#endif
/*
 • Class:     org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader

 • Method:    getAvailabilityAndBacklog

 • Signature: (JI)I

 */
JNIEXPORT jint JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_getAvailabilityAndBacklog
  (JNIEnv *, jobject, jlong, jint);

/*
 • Class:     org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader

 • Method:    getNextBuffer

 • Signature: (J)I

 */
JNIEXPORT jint JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_getNextBuffer
  (JNIEnv *, jobject, jlong);

/*
 • Class:     org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader

 • Method:    createNativeCreditBasedSequenceNumberingViewReader

 • Signature: (JJLjava/lang/String;I)J

 */
JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_createNativeCreditBasedSequenceNumberingViewReader
  (JNIEnv *, jobject, jlong, jlong, jstring, jint);

/*
 • Class:     org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader

 • Method:    firstDataAvailableNotification

 • Signature: (J)V

 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_firstDataAvailableNotification
  (JNIEnv *, jobject, jlong);

/*
 • Class:     org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader

 • Method:    destroyNativeNettyBufferPool

 • Signature: (J)V

 */
JNIEXPORT void JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_destroyNativeNettyBufferPool
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif