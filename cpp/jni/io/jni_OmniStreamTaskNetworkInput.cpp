/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput.h"
#include "io/StreamOneInputProcessor.h"
#include "task/StreamTask.h"


JNIEXPORT jobject JNICALL Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput_TNELProcessBuffer
        (JNIEnv *env, jobject input, jlong omniStreamTaskRef, jlong omniInputProcessorRef, jlong address, jint offset, jint numBytes, jlong channelInfo)
{
    auto *streamTask = reinterpret_cast<omnistream::datastream::StreamTask *>(omniStreamTaskRef);
    auto *processor = reinterpret_cast<omnistream::datastream::StreamOneInputProcessor *>(omniInputProcessorRef);

    auto buffAddress = reinterpret_cast<uint8_t *>(address);

    LOG(" Inside Java_com_huawei_omniruntime_flink_streaming_runtime_io_OmniStreamTaskNetworkInput_TNELProcessBuffer buffAddress + offset " +
        std::to_string(reinterpret_cast<long>(buffAddress + offset)) + " numBytes " + std::to_string(numBytes))

    auto myClass = env->FindClass("com/huawei/omniruntime/flink/streaming/runtime/io/TNELProcessState");
    auto constructor = env->GetMethodID(myClass, "<init>", "(IJ)V");
    int32_t inputNumber = 0;
    auto status = processor->processInput(buffAddress + offset, numBytes, channelInfo, inputNumber);
    streamTask->resetOutputBufferAndRecordWriter();
    auto myObject = env->NewObject(myClass, constructor, status, inputNumber);
    return myObject;
}