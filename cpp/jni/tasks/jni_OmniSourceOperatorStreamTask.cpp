/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "com_huawei_omniruntime_flink_runtime_tasks_OmniSourceOperatorStreamTaskV2.h"
#include "streaming/runtime/tasks/StreamTask.h"

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
