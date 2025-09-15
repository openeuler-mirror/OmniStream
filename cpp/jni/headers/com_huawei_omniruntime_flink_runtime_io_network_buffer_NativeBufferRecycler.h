/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler
#define _Included_com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler
 * Method:    freeNativeByteBuffer
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler_freeNativeByteBuffer
  (JNIEnv *, jobject, jlong, jlong);

#ifdef __cplusplus
}
#endif
#endif