/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <taskmanager/OmniTask.h>
#include <nlohmann/json.hpp>
#include <string>
#include "common.h"
#include "org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader.h"
#include "com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler.h"
#include "runtime/io/network/netty/OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/executiongraph/descriptor/ResultPartitionIDPOD.h"

JNIEXPORT jlong JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_createNativeCreditBasedSequenceNumberingViewReader
  (JNIEnv *jniEnv, jobject input, jlong nativeTask, jlong resultBufferAddress, jstring partitionIdJson, jint subPartitionId)
{
    const char* paritionIdChars  = jniEnv->GetStringUTFChars(partitionIdJson, nullptr);
    std::string paritionIdStr(paritionIdChars);
    jniEnv->ReleaseStringUTFChars(partitionIdJson, paritionIdChars);

    nlohmann::json partitionId = nlohmann::json::parse(paritionIdStr);
    omnistream::ResultPartitionIDPOD partitionIdPOD = partitionId;


    auto task = reinterpret_cast<omnistream::OmniTask *>(nativeTask);
    return task->createNativeCreditBasedSequenceNumberingViewReader(resultBufferAddress, partitionIdPOD, subPartitionId);
}


JNIEXPORT jint JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_getAvailabilityAndBacklog
  (JNIEnv *jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef, jint numCreditsAvailable)
{
    auto task = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader *>(creditBasedSequenceNumberingViewReaderRef);
    return task->getAvailabilityAndBacklog();
}

JNIEXPORT jint JNICALL Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_getNextBuffer
  (JNIEnv * jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef)
{
    auto task = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader *>(creditBasedSequenceNumberingViewReaderRef);
    return task->getNextBuffer();
}



JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_io_network_buffer_NativeBufferRecycler_freeNativeByteBuffer
  (JNIEnv *jniEnv, jobject input,  jlong creditBasedSequenceNumberingViewReaderRef, jlong nativeByteBufferAddressRef)
{
    auto omniCreditBasedSequenceNumberingViewReader =
        reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader *>
    (creditBasedSequenceNumberingViewReaderRef);
    omniCreditBasedSequenceNumberingViewReader->RecycleNettyBuffer(nativeByteBufferAddressRef);
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_firstDataAvailableNotification
(JNIEnv* jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef)
{
    if (creditBasedSequenceNumberingViewReaderRef == -1) {
        return;
    }
    auto task = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(
        creditBasedSequenceNumberingViewReaderRef);
    task->notifyDataAvailable();
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_io_network_netty_OmniCreditBasedSequenceNumberingViewReader_destroyNativeNettyBufferPool
(JNIEnv* jniEnv, jobject input, jlong creditBasedSequenceNumberingViewReaderRef)
{
    // it is possible OmniCreditBasedSequenceNumberingViewReader is destroyed already
    auto task = reinterpret_cast<omnistream::OmniCreditBasedSequenceNumberingViewReader*>(
        creditBasedSequenceNumberingViewReaderRef);
    task->DestroyNettyBufferPool();
}
