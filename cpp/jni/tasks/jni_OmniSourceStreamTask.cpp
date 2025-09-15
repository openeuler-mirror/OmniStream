/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include <tasks/OmniSourceStreamTask.h>
#include "common.h"

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2.h"
#include "task/StreamTask.h"

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2_runStreamSource
        (JNIEnv * env, jobject obj, jlong omniStreamTaskRef)
{
    LOG("runStreamSource at " << reinterpret_cast<long>(omniStreamTaskRef));
    auto pTask = reinterpret_cast<omnistream::datastream::StreamTask *>(omniStreamTaskRef);
    auto source = reinterpret_cast<omnistream::StreamSource<Object> *>(pTask->getMainOperator());
    jclass task = env->GetObjectClass(obj);
    auto methodID = env->GetMethodID(task, "process", "()V");
    source->setProcessArgs(methodID, env, obj);
    source->run();
    return 0L;
}

JNIEXPORT jlong JNICALL Java_com_huawei_omniruntime_flink_runtime_tasks_OmniSourceStreamTaskV2_cancelStreamSource
        (JNIEnv *, jobject, jlong omniStreamTaskRef)
{
    LOG("cancelStreamSource at " << reinterpret_cast<long>(omniStreamTaskRef));
    auto pTask = reinterpret_cast<omnistream::datastream::StreamTask *>(omniStreamTaskRef);
    auto source = reinterpret_cast<omnistream::StreamSource<Object> *>(pTask->getMainOperator());
    source->cancel();
    return 0L;
}

JNIEXPORT void JNICALL Java_org_apache_flink_streaming_runtime_tasks_OmniSourceStreamTask_resetOutputBufferAndRecordWriter
        (JNIEnv *, jobject, jlong omniStreamTaskRef) {
    auto pTask = reinterpret_cast<omnistream::datastream::StreamTask *>(omniStreamTaskRef);
    pTask->resetOutputBufferAndRecordWriter();
}

