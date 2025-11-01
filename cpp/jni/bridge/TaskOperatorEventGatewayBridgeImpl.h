/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 */

#ifndef TASKOPERATOREVENTGATEWAYBRIDGEIMPL_H
#define TASKOPERATOREVENTGATEWAYBRIDGEIMPL_H

#include <common.h>
#include <jni.h>
#include <common/global.h>
#include <state/bridge/TaskOperatorEventGatewayBridge.h>

#include "state/bridge/OmniTaskBridge.h"


class TaskOperatorEventGatewayBridgeImpl : public omnistream::TaskOperatorEventGatewayBridge {
public:
    explicit TaskOperatorEventGatewayBridgeImpl(jobject m_globalTaskOperatorEventGateWayRef)
    {
        this->m_globalTaskOperatorEventGateWayRef=m_globalTaskOperatorEventGateWayRef;
    }
    void sendOperatorEventToCoordinator(std::string operatorid, std::string event) override
    {
        JNIEnv* env;
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            return;
        }

        if (m_globalTaskOperatorEventGateWayRef != nullptr) {
            jclass TaskOperatorEventGateWayClass = env->GetObjectClass(m_globalTaskOperatorEventGateWayRef);
            if (TaskOperatorEventGateWayClass == nullptr) {
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jmethodID sendOperatorEventToCoordinatorMethodId = env->GetMethodID(TaskOperatorEventGateWayClass, "sendOperatorEventToCoordinator",
                "(Ljava/lang/String;Ljava/lang/String;)V");
            if (sendOperatorEventToCoordinatorMethodId == nullptr) {
                env->DeleteLocalRef(TaskOperatorEventGateWayClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring operatoridstr = env->NewStringUTF(operatorid.c_str());
            jstring eventstr = env->NewStringUTF(event.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(m_globalTaskOperatorEventGateWayRef, sendOperatorEventToCoordinatorMethodId, operatoridstr, eventstr);

            if (env->ExceptionCheck()) {
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            env->DeleteLocalRef(TaskOperatorEventGateWayClass);
            env->DeleteLocalRef(eventstr);
            env->DeleteLocalRef(operatoridstr);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }

        g_OmniStreamJVM->DetachCurrentThread();
    }
public:
    jobject m_globalTaskOperatorEventGateWayRef;
};

#endif // TASKOPERATOREVENTGATEWAYBRIDGEIMPL_H
