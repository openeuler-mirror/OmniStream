/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef _Included_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput
#define _Included_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput

#include <jni.h>
/* Header for class com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput
 * Method:    emitNextNative
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput_emitNextNative
  (JNIEnv *, jclass, jlong);

/*
 * Class:     com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput
 * Method:    getSourceReaderAvailableNative
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput_getSourceReaderAvailableNative
  (JNIEnv *, jclass, jlong);

#ifdef __cplusplus
}
#endif
#endif