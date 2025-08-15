/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2.h"
#include "task/StreamTask.h"

#include "streaming/api/operators/SourceOperator.h"

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2_getNativeSourceOperator
        (JNIEnv *, jclass, jlong omniStreamTaskRef)
{
    auto *pTask = reinterpret_cast<omnistream::datastream::StreamTask *>(omniStreamTaskRef);
    StreamOperator *sourceOperator = pTask->getMainOperator();
    return reinterpret_cast<long>(sourceOperator);
}

JNIEXPORT void JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2_handleOperatorEvent(JNIEnv *env, jclass, jlong omniSourceOperatorRef, jstring eventDesc)
{
    auto *pSource = reinterpret_cast<SourceOperator<>*>(omniSourceOperatorRef);
    const char *cStrTDD = (env)->GetStringUTFChars(eventDesc, 0);
    std::string eventString(cStrTDD);
    pSource->handleOperatorEvent(eventString);
    (env)->ReleaseStringUTFChars(eventDesc, cStrTDD);
}
