/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput.h"
#include "task/StreamTask.h"
#include "streaming/api/operators/SourceOperator.h"

JNIEXPORT jint JNICALL Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput_emitNextNative
        (JNIEnv *, jclass, jlong nativeStreamTaskRef)
{
    auto *streamTask = reinterpret_cast<omnistream::datastream::StreamTask *>(nativeStreamTaskRef);
    auto *sourceOperator = reinterpret_cast<SourceOperator<> *>(streamTask->getMainOperator());
    auto dataOutput = sourceOperator->getDataStreamOutput();
    DataInputStatus status = sourceOperator->emitNext(dataOutput);

    streamTask->resetOutputBufferAndRecordWriter();
    return static_cast<jint>(status);
}

JNIEXPORT jboolean JNICALL Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskSourceInput_getSourceReaderAvailableNative
        (JNIEnv *, jclass, jlong nativeStreamTaskRef)
{
    return static_cast<jboolean>(true);
}
