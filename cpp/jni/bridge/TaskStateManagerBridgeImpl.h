/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 */


#ifndef TASKSTATEMANAGERBRIDGEIMPL_H
#define TASKSTATEMANAGERBRIDGEIMPL_H

#include <fstream>
#include <jni.h>
#include <state/bridge/TaskStateManagerBridge.h>
#include <common/global.h>

#include "runtime/state/SnapshotResult.h"

namespace omnistream {

class TaskStateManagerBridgeImpl : public TaskStateManagerBridge {
public:
    explicit TaskStateManagerBridgeImpl(jobject mGlobalTaskStateMgrRef)
    {
        this->m_globalTaskStateMgrRef=mGlobalTaskStateMgrRef;
    }
    // ~TaskStateManagerBridgeImpl() override;
    void ReportTaskStateSnapshots(std::string &checkpointMetaDataJson,
         std::string &checkpointMetricsJson,
         std::string &acknowledgedStateJson,
         std::string &localStateJson) override
    {
        JNIEnv* env;
        // Attach the current thread to the Java VM
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::ReportTaskStateSnapshots");
            return;
        }

        if (m_globalTaskStateMgrRef != nullptr) {
            jclass taskStateManagerWrapperClass = env->GetObjectClass(m_globalTaskStateMgrRef);
            if (taskStateManagerWrapperClass == nullptr) {
                GErrorLog("Error: Could not get TaskStateManagerWrapper class.");
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            // 2. Get the method ID for reportTaskStateSnapshots
            // The signature is (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
            // V for void return type, and Ljava/lang/String; for each String argument
            jmethodID reportMethodId = env->GetMethodID(taskStateManagerWrapperClass, "reportTaskStateSnapshots",
                "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
            if (reportMethodId == nullptr) {
                GErrorLog("Error: Could not find method reportTaskStateSnapshots.");
                env->DeleteLocalRef(taskStateManagerWrapperClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring checkpointMetaData = env->NewStringUTF(checkpointMetaDataJson.c_str());
            jstring checkpointMetrics = env->NewStringUTF(checkpointMetricsJson.c_str());
            jstring acknowledgedState = env->NewStringUTF(acknowledgedStateJson.c_str());
            jstring localState = env->NewStringUTF(localStateJson.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(m_globalTaskStateMgrRef, reportMethodId, checkpointMetaData,
                checkpointMetrics, acknowledgedState, localState);

            // 4. Check for any pending exceptions after the call (optional but good practice)
            if (env->ExceptionCheck()) {
                GErrorLog("Error: Exception occurred during Java method invocation.");
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            // 5. Clean up local references (important for performance and memory management)
            env->DeleteLocalRef(taskStateManagerWrapperClass);
            env->DeleteLocalRef(checkpointMetaData);
            env->DeleteLocalRef(checkpointMetrics);
            env->DeleteLocalRef(acknowledgedState);
            env->DeleteLocalRef(localState);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }

        g_OmniStreamJVM->DetachCurrentThread();
    };
    void notifyCheckpointAborted(std::string checkpointId) override
    {
        JNIEnv* env;
        // Attach the current thread to the Java VM
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::ReportTaskStateSnapshots");
            return;
        }

        if (m_globalTaskStateMgrRef != nullptr) {
            jclass taskStateManagerWrapperClass = env->GetObjectClass(m_globalTaskStateMgrRef);
            if (taskStateManagerWrapperClass == nullptr) {
                GErrorLog("Error: Could not get TaskStateManagerWrapper class.");
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            // 2. Get the method ID for reportTaskStateSnapshots
            // The signature is (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
            // V for void return type, and Ljava/lang/String; for each String argument
            jmethodID notifyCheckpointAbortedMethodId = env->GetMethodID(taskStateManagerWrapperClass, "notifyCheckpointAborted",
                "(Ljava/lang/String;)V");
            if (notifyCheckpointAbortedMethodId == nullptr) {
                GErrorLog("Error: Could not find method notifyCheckpointAborted.");
                env->DeleteLocalRef(taskStateManagerWrapperClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring checkpointIdstr = env->NewStringUTF(checkpointId.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(m_globalTaskStateMgrRef, notifyCheckpointAbortedMethodId, checkpointIdstr);

            // 4. Check for any pending exceptions after the call (optional but good practice)
            if (env->ExceptionCheck()) {
                GErrorLog("Error: Exception occurred during Java method invocation.");
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            // 5. Clean up local references (important for performance and memory management)
            env->DeleteLocalRef(taskStateManagerWrapperClass);
            env->DeleteLocalRef(checkpointIdstr);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }

        g_OmniStreamJVM->DetachCurrentThread();
    }

    void NotifyCheckpointComplete(std::string checkpointId) override
    {
        JNIEnv* env;
        // Attach the current thread to the Java VM
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::ReportTaskStateSnapshots");
            return;
        }

        if (m_globalTaskStateMgrRef != nullptr) {
            jclass taskStateManagerWrapperClass = env->GetObjectClass(m_globalTaskStateMgrRef);
            if (taskStateManagerWrapperClass == nullptr) {
                GErrorLog("Error: Could not get TaskStateManagerWrapper class.");
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            // 2. Get the method ID for reportTaskStateSnapshots
            // The signature is (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
            // V for void return type, and Ljava/lang/String; for each String argument
            jmethodID notifyCheckpointCompleteMethodId = env->GetMethodID(taskStateManagerWrapperClass, "notifyCheckpointComplete",
                "(Ljava/lang/String;)V");
            if (notifyCheckpointCompleteMethodId == nullptr) {
                GErrorLog("Error: Could not find method notifyCheckpointAborted.");
                env->DeleteLocalRef(taskStateManagerWrapperClass); // Clean up local ref
                g_OmniStreamJVM->DetachCurrentThread();
                return;
            }

            jstring checkpointIdstr = env->NewStringUTF(checkpointId.c_str());

            // 3. Invoke the Java method
            env->CallVoidMethod(m_globalTaskStateMgrRef, notifyCheckpointCompleteMethodId, checkpointIdstr);

            // 4. Check for any pending exceptions after the call (optional but good practice)
            if (env->ExceptionCheck()) {
                GErrorLog("Error: Exception occurred during Java method invocation.");
                env->ExceptionDescribe(); // Print exception details to stderr
                env->ExceptionClear();    // Clear the exception
            }

            // 5. Clean up local references (important for performance and memory management)
            env->DeleteLocalRef(taskStateManagerWrapperClass);
            env->DeleteLocalRef(checkpointIdstr);
        } else {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        }
        g_OmniStreamJVM->DetachCurrentThread();
    }

private:
    jobject m_globalTaskStateMgrRef;
};

} // omnistream

#endif // TASKSTATEMANAGERBRIDGEIMPL_H
