/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 */

#include "OmniTaskBridgeImpl2.h"

void OmniTaskBridgeImpl2::declineCheckpoint(std::string &checkpointIDJson, std::string &failure_reasonJson,
    std::string &exceptionJson)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        return;
    }

    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jmethodID declinedMethodId = env->GetMethodID(omniTaskWrapperClass, "declineCheckpoint",
                                                      "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");
        if (declinedMethodId == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jstring checkpointid = env->NewStringUTF(checkpointIDJson.c_str());
        jstring failurereason = env->NewStringUTF(failure_reasonJson.c_str());
        jstring exceptionStr = env->NewStringUTF(exceptionJson.c_str());

        // 3. Invoke the Java method
        env->CallVoidMethod(m_globalOmniTaskRef, declinedMethodId, checkpointid, failurereason, exceptionStr);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
        }

        env->DeleteLocalRef(omniTaskWrapperClass);
        env->DeleteLocalRef(checkpointid);
        env->DeleteLocalRef(failurereason);
        env->DeleteLocalRef(exceptionStr);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

OmniTaskBridgeImpl2::~OmniTaskBridgeImpl2()
{
    if (m_globalOmniTaskRef != nullptr) {
        JNIEnv* env;
        // Attach the current thread to the Jvm
        jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
        if (res != JNI_OK) {
            return;
        }
        env->DeleteGlobalRef(m_globalOmniTaskRef);
        m_globalOmniTaskRef = nullptr; // Clear the reference
        g_OmniStreamJVM->DetachCurrentThread();
    }
}

std::shared_ptr<SnapshotResult<StreamStateHandle>> OmniTaskBridgeImpl2::CallMaterializeMetaData(
    jlong checkpointId,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
    std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
    CheckpointOptions *checkpointOptions,
    std::string keySerializer)
{
    if (m_globalOmniTaskRef == nullptr) {
        GErrorLog("StreamTask is not registered in TaskStateManagerBridgeImpl::CallMaterializeMetaData");
        return nullptr;
    }

    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::CallMaterializeMetaData");
        return nullptr;
    }
    if (checkpointOptions == nullptr) {
        GErrorLog("checkpointOptions is nullptr in TaskStateManagerBridgeImpl::CallMaterializeMetaData");
        return nullptr;
    }
    nlohmann::json stateMetaInfoJson = nlohmann::json::array();
    for (const auto& snapshot : snapshots) {
        nlohmann::json jsonObj;
        jsonObj["name"] = snapshot->getName();
        jsonObj["backendStateType"] =
        static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj["options"] = snapshot->getOptionsImmutable();
        jsonObj["serializer"] = snapshot->getSerializerJson();
        jsonObj["keySerializer"] = keySerializer;
        stateMetaInfoJson.push_back(std::move(jsonObj));
    }
    std::string stateMetaInfoStr = stateMetaInfoJson.dump();

    std::string localRecoveryConfigStr = "{}";
    if (localRecoveryConfig != nullptr && localRecoveryConfig->IsLocalRecoveryEnabled()){
        try {
            nlohmann::json localRecoveryJson;
            auto directoryProvider = localRecoveryConfig->GetLocalStateDirectoryProvider();

            // 序列化 allocationBaseDirs_
            nlohmann::json baseDirsArray = nlohmann::json::array();
            for (const auto& path : directoryProvider->GetPaths()) {
                baseDirsArray.push_back(path.string()); // 将filesystem::path转换为字符串
            }
            localRecoveryJson["allocationBaseDirs"] = baseDirsArray;

            // 序列化 jobID_
            localRecoveryJson["jobID"] = directoryProvider->GetJobIdHexStr();

            // 序列化 jobVertexID_
            localRecoveryJson["jobVertexID"] = directoryProvider->GetVertexIdHexStr();

            // 序列化 subtaskIndex_
            localRecoveryJson["subtaskIndex"] = directoryProvider->GetSubIndex();

            localRecoveryConfigStr = localRecoveryJson.dump();

        } catch (const std::exception& e) {
            std::stringstream errorMsg;
            errorMsg << "Failed to serialize localRecoveryConfig: " << e.what();
            GErrorLog(errorMsg.str());
            localRecoveryConfigStr = "{}"; // 序列化失败时使用空JSON
        }

    }
    nlohmann::json jcheckpointOptions = checkpointOptions->ToJson();
    std::string checkpointOptionsStr = jcheckpointOptions.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "materializeMetaData",
        "(JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)Lorg/apache/flink/runtime/state/SnapshotResult;"
    );
    jstring jStateMetaInfoStr = env->NewStringUTF(stateMetaInfoStr.c_str());
    jstring jLocalRecoveryConfigStr = env->NewStringUTF(localRecoveryConfigStr.c_str());
    jstring jcheckpointOptionsStr = env->NewStringUTF(checkpointOptionsStr.c_str());
    jobject resultObj = env->CallObjectMethod(m_globalOmniTaskRef, mid, checkpointId, jStateMetaInfoStr, jLocalRecoveryConfigStr, jcheckpointOptionsStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jStateMetaInfoStr);
        env->DeleteLocalRef(jLocalRecoveryConfigStr);
        env->DeleteLocalRef(cls);
        env->DeleteLocalRef(jcheckpointOptionsStr);
        throw std::runtime_error("Failed to call materializeMetaData");
    }
    env->DeleteLocalRef(jStateMetaInfoStr);
    env->DeleteLocalRef(jLocalRecoveryConfigStr);
    env->DeleteLocalRef(cls);
    env->DeleteLocalRef(jcheckpointOptionsStr);

    return helper.ConvertSnapshotResult(env, resultObj);
}

std::shared_ptr<SnapshotResult<OperatorStateHandle>> OmniTaskBridgeImpl2::CallMaterializeOperatorMetaData(
    jlong checkpointId_,
    CheckpointOptions* checkpointOptions_,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& operatorStateMetaInfoSnapshots_,
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& broadcastStateMetaInfoSnapshots_) {

    if (m_globalOmniTaskRef == nullptr) {
        GErrorLog("StreamTask is not registered in TaskStateManagerBridgeImpl::CallMaterializeOperatorMetaData");
        return nullptr;
    }

    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        GErrorLog("Failed to attach C++ thread to JVM inside TaskStateManagerBridgeImpl::CallMaterializeOperatorMetaData");
        return nullptr;
    }
    if (checkpointOptions_ == nullptr) {
        GErrorLog("checkpointOptions is nullptr in TaskStateManagerBridgeImpl::CallMaterializeOperatorMetaData");
        return nullptr;
    }
    nlohmann::json operatorStateMetaInfoJson = nlohmann::json::array();
    nlohmann::json broadcastStateMetaInfoJson = nlohmann::json::array();
    for (const auto& snapshot : operatorStateMetaInfoSnapshots_) {
        try {
            if (snapshot == nullptr) {
                continue;
            }
        
            nlohmann::json jsonObj;
            jsonObj["name"] = snapshot->getName();
            jsonObj["backendStateType"] = static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
            jsonObj["options"] = snapshot->getOptionsImmutable();
            jsonObj["serializer"] = snapshot->getSerializerJson();
            operatorStateMetaInfoJson.push_back(std::move(jsonObj));
        } catch (const std::exception& e) {
            GErrorLog("OmniTaskBridgeImpl2::CallMaterializeOperatorMetaData error " + std::string(e.what()));
        }
    }
    for (const auto& snapshot : broadcastStateMetaInfoSnapshots_) {
        if (snapshot == nullptr) {
            continue;
        }
        nlohmann::json jsonObj;
        jsonObj["name"] = snapshot->getName();
        jsonObj["backendStateType"] = static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj["options"] = snapshot->getOptionsImmutable();
        jsonObj["serializer"] = snapshot->getSerializerJson();
        broadcastStateMetaInfoJson.push_back(std::move(jsonObj));
    }
    std::string operatorStateMetaInfoStr = operatorStateMetaInfoJson.dump();
    std::string broadcastStateMetaInfoStr = broadcastStateMetaInfoJson.dump();

    nlohmann::json jCheckpointOptions = checkpointOptions_->ToJson();
    std::string checkpointOptionsStr = jCheckpointOptions.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "materializeOperatorMetaData",
        "(JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)Lorg/apache/flink/runtime/state/SnapshotResult;"
    );
    jstring jOperatorStateMetaInfoStr = env->NewStringUTF(operatorStateMetaInfoStr.c_str());
    jstring jBroadcastStateMetaInfoStr = env->NewStringUTF(broadcastStateMetaInfoStr.c_str());
    jstring jCheckpointOptionsStr = env->NewStringUTF(checkpointOptionsStr.c_str());
    jobject resultObj = env->CallObjectMethod(m_globalOmniTaskRef, mid, checkpointId_, jCheckpointOptionsStr, jOperatorStateMetaInfoStr, jBroadcastStateMetaInfoStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jOperatorStateMetaInfoStr);
        env->DeleteLocalRef(jBroadcastStateMetaInfoStr);
        env->DeleteLocalRef(cls);
        env->DeleteLocalRef(jCheckpointOptionsStr);
        throw std::runtime_error("Failed to call materializeOperatorMetaData");
    }
    env->DeleteLocalRef(jOperatorStateMetaInfoStr);
    env->DeleteLocalRef(jBroadcastStateMetaInfoStr);
    env->DeleteLocalRef(cls);
    env->DeleteLocalRef(jCheckpointOptionsStr);

    std::shared_ptr<SnapshotResult<StreamStateHandle>> resultHandle = helper.ConvertSnapshotResult(env, resultObj);
    std::shared_ptr<SnapshotResult<OperatorStateHandle>> operatorStateHandle = std::dynamic_pointer_cast<SnapshotResult<OperatorStateHandle>>(resultHandle);

    return operatorStateHandle;
}

jobject OmniTaskBridgeImpl2::CallUploadFilesToCheckpointFs(const std::vector<Path>& filePaths,
                                                           int numberOfSnapshottingThreads)
{
    JNIEnv* env = nullptr;
    jint attachRes = 0;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        GErrorLog("Failed to attach C++ thread to JVM inside CallUploadFilesToCheckpointFs");
        return nullptr;
    }

    nlohmann::json jPaths = nlohmann::json::array();
    for (const auto &p : filePaths) {
        jPaths.push_back(p.toString());
    }
    std::string pathsJsonStr = jPaths.dump();

    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "uploadFilesToCheckpointFs", "(Ljava/lang/String;I)Ljava/util/List;");
    jstring jPathsJson = env->NewStringUTF(pathsJsonStr.c_str());
    jobject jResult = env->CallObjectMethod(m_globalOmniTaskRef, mid, jPathsJson, numberOfSnapshottingThreads);

    env->DeleteLocalRef(jPathsJson);
    env->DeleteLocalRef(cls);

    return jResult;
}

bool OmniTaskBridgeImpl2::CallDownloadFileToLocal(const StreamStateHandle &cppHandle,
    const std::string &restoreInstancePath)
{
    JNIEnv* env = nullptr;
    jint attachRes = 0;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        GErrorLog("Failed to attach C++ thread to JVM inside CallDownloadFileToLocal");
        return false;
    }

    jobject restoreFileHandle = helper.ConvertToJavaStreamStateHandle(env, cppHandle);
    if (restoreFileHandle == nullptr) {
        GErrorLog("Failed to convert to java stream state handle");
        return false;
    }
    jobject restoreTargetPath = helper.ConvertPathStrToJavaPath(env, restoreInstancePath);
    if (restoreTargetPath == nullptr) {
        GErrorLog("Failed to convert to java path");
        return false;
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    // 获取CallDownloadFileToLocal方法ID
    jmethodID mid = env->GetMethodID(cls, "callDownloadFileToLocal",
        "(Lorg/apache/flink/runtime/state/StreamStateHandle;Ljava/nio/file/Path;)Z");
    auto jResult = env->CallBooleanMethod(m_globalOmniTaskRef, mid, restoreFileHandle, restoreTargetPath);
    env->DeleteLocalRef(cls);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return false;
    }
    return jResult == JNI_TRUE;
}

std::vector<StateMetaInfoSnapshot> OmniTaskBridgeImpl2::readMetaData(const std::string &metaStateHandle)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        return {};
    }

    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jmethodID readMetaMethodId = env->GetMethodID(omniTaskWrapperClass, "readMetaData",
                                                      "(Ljava/lang/String;)Ljava/lang/String;");
        if (readMetaMethodId == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jstring msHandle = env->NewStringUTF(metaStateHandle.c_str());

        // Invoke the Java method
        jstring result = (jstring) env->CallObjectMethod(m_globalOmniTaskRef, readMetaMethodId, msHandle);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
        }

        // Convert jstring to std::string
        const char* strChars = env->GetStringUTFChars(result, nullptr);
        std::string cppResult(strChars);
        env->ReleaseStringUTFChars(result, strChars);
        g_OmniStreamJVM->DetachCurrentThread();

        return helper.convertResult(cppResult);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        return {};
    }
}

std::vector<StateMetaInfoSnapshot> OmniTaskBridgeImpl2::readOperatorMetaData(const std::string &metaStateHandle)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        return {};
    }

    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jmethodID readMetaMethodId = env->GetMethodID(omniTaskWrapperClass, "readOperatorMetaData",
                                                      "(Ljava/lang/String;)Ljava/lang/String;");
        if (readMetaMethodId == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return {};
        }

        jstring msHandle = env->NewStringUTF(metaStateHandle.c_str());

        // Invoke the Java method
        jstring result = (jstring) env->CallObjectMethod(m_globalOmniTaskRef, readMetaMethodId, msHandle);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
        }

        // Convert jstring to std::string
        const char* strChars = env->GetStringUTFChars(result, nullptr);
        std::string cppResult(strChars);
        env->ReleaseStringUTFChars(result, strChars);
        g_OmniStreamJVM->DetachCurrentThread();

        return helper.convertResult(cppResult);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        return {};
    }
}

std::string OmniTaskBridgeImpl2::restoreOperatorStreamState(const std::string &stateHandle)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        GErrorLog("Error: Could not AttachCurrentThread for JNI call");
        return "";
    }

    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return "";
        }

        jmethodID stateRestoreMethodId = env->GetMethodID(omniTaskWrapperClass, "operatorStateRestore",
                                                      "(Ljava/lang/String;)Ljava/lang/String;");
        if (stateRestoreMethodId == nullptr) {
            GErrorLog("Error: Could not get operatorStateRestore method for JNI call");
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return "";
        }

        jstring msHandle = env->NewStringUTF(stateHandle.c_str());

        // Invoke the Java method
        jstring result = (jstring) env->CallObjectMethod(m_globalOmniTaskRef, stateRestoreMethodId, msHandle);

        if (env->ExceptionCheck()) {
            GErrorLog("Error: operatorStateRestore method threw an exception");
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
            g_OmniStreamJVM->DetachCurrentThread();
            return "";
        }

        // Convert jstring to std::string
        const char* strChars = env->GetStringUTFChars(result, nullptr);
        std::string cppResult(strChars);
        INFO_RELEASE("operatorStateRestore result: " << cppResult);
        
        env->ReleaseStringUTFChars(result, strChars);
        g_OmniStreamJVM->DetachCurrentThread();
        return cppResult;
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        return "";
    }
}

void OmniTaskBridgeImpl2::getKeyGroupEntries(jobject inputStream,
    int &currentKvStateId, bool isUsingKeyGroupCompression, std::vector<KeyGroupEntry> &entries)
{
    entries.clear();
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        GErrorLog("Error: getKeyGroupEntries could not AttachCurrentThread for JNI call");
        return;
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            GErrorLog("Error: getKeyGroupEntries could not GetObjectClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jclass entryWrapperClass = env->FindClass("com/huawei/omniruntime/flink/runtime/restore/KeyGroupEntryWrapper");
        jclass entryClass = env->FindClass("com/huawei/omniruntime/flink/runtime/restore/KeyGroupEntry");
        if (unlikely(entryWrapperClass == nullptr || entryClass == nullptr)) {
            GErrorLog("Error: getKeyGroupEntries could not FindClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jfieldID currentKvStateIdField = env->GetFieldID(entryWrapperClass, "currentKvStateId", "I");
        jfieldID entriesField = env->GetFieldID(entryWrapperClass, "entries", "[Lcom/huawei/omniruntime/flink/runtime/restore/KeyGroupEntry;");
        jfieldID entryKvStateIdField = env->GetFieldID(entryWrapperClass, "kvStateId", "I");
        jfieldID entryCountField = env->GetFieldID(entryWrapperClass, "count", "I");
        if (unlikely(currentKvStateIdField == nullptr || entriesField == nullptr
            || entryKvStateIdField == nullptr || entryCountField == nullptr)) {
            GErrorLog("Error: getKeyGroupEntries entryWrapperClass could not GetFieldID for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jfieldID entryKeyField = env->GetFieldID(entryClass, "key", "[B");
        jfieldID entryValueField = env->GetFieldID(entryClass, "value", "[B");
        if (unlikely(entryKeyField == nullptr || entryValueField == nullptr)) {
            GErrorLog("Error: getKeyGroupEntries entryClass could not GetFieldID for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "getKeyGroupEntries",
            "(Lorg/apache/flink/core/fs/FSDataInputStream;IZ)Lcom/huawei/omniruntime/flink/runtime/restore/KeyGroupEntryWrapper;");

        jint jCurrentKvStateId = static_cast<jint>(currentKvStateId);
        jboolean jIsUsingKeyGroupCompression = static_cast<jboolean>(isUsingKeyGroupCompression);

        // Invoke the Java method
        jobject result = env->CallObjectMethod(m_globalOmniTaskRef,
            mid, inputStream, jCurrentKvStateId, jIsUsingKeyGroupCompression);

        if (env->ExceptionCheck()) {
            env->ExceptionDescribe(); // Print exception details to stderr
            env->ExceptionClear();    // Clear the exception
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        if (result == nullptr) {
            GErrorLog("Error: getKeyGroupEntries get null result for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }

        currentKvStateId = env->GetIntField(result, currentKvStateIdField);
        int kvStateId = env->GetIntField(result, entryKvStateIdField);
        int count = env->GetIntField(result, entryCountField);

        jobjectArray entriesArray = static_cast<jobjectArray>(env->GetObjectField(result, entriesField));

        for (int i = 0; i < count; i++) {
            jobject entry = env->GetObjectArrayElement(entriesArray, i);
            jbyteArray keyArray = static_cast<jbyteArray>(env->GetObjectField(entry, entryKeyField));
            jbyteArray valueArray = static_cast<jbyteArray>(env->GetObjectField(entry, entryValueField));

            entries.emplace_back(KeyGroupEntry(kvStateId, std::move(helper.jbyteArrayToVector(env, keyArray)),
                std::move(helper.jbyteArrayToVector(env, valueArray))));
            
            if (entry) env->DeleteLocalRef(entry);
            if (keyArray) env->DeleteLocalRef(keyArray);
            if (valueArray) env->DeleteLocalRef(valueArray);
        }
        env->DeleteLocalRef(entriesArray);
        env->DeleteLocalRef(entryWrapperClass);
        env->DeleteLocalRef(entryClass);
        env->DeleteLocalRef(result);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

jobject OmniTaskBridgeImpl2::getSavepointInputStream(const std::string &metaStateHandle)
{
    JNIEnv* env;
    jobject inputStream = nullptr;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: getSavepointInputStream could not AttachCurrentThread for JNI call");
        throw std::runtime_error("getSavepointInputStream could not AttachCurrentThread for JNI call");
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: getSavepointInputStream could not GetObjectClass for JNI call");
            throw std::runtime_error("getSavepointInputStream could not GetObjectClass for JNI call");
        }
        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "getSavepointInputStream",
            "(Ljava/lang/String;)Lorg/apache/flink/core/fs/FSDataInputStream;");
        if (mid == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: getSavepointInputStream could not get methodID for JNI call");
            throw std::runtime_error("getSavepointInputStream could not get methodID for JNI call");
        }
        jstring msHandle = env->NewStringUTF(metaStateHandle.c_str());
        inputStream = env->CallObjectMethod(m_globalOmniTaskRef, mid, msHandle);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: Failed to call SavepointInputStream get method");
            throw std::runtime_error("Failed to call SavepointInputStream get method");
        }
        env->DeleteLocalRef(msHandle);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
        throw std::runtime_error("Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
    return inputStream;
}

bool OmniTaskBridgeImpl2::isUsingKeyGroupCompression(jobject inputStream)
{
    JNIEnv* env;
    bool result = false;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: isUsingKeyGroupCompression could not AttachCurrentThread for JNI call");
        return false;
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            INFO_RELEASE("Error: isUsingKeyGroupCompression could not GetObjectClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return false;
        }
        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "isUsingKeyGroupCompression",
            "(Lorg/apache/flink/core/fs/FSDataInputStream;)Z");
        if (mid == nullptr) {
            INFO_RELEASE("Error: isUsingKeyGroupCompression could not GetMethodID for JNI call");
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return false;
        }
        auto ret = env->CallBooleanMethod(m_globalOmniTaskRef, mid, inputStream);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            g_OmniStreamJVM->DetachCurrentThread();
            throw std::runtime_error("Failed to call SavepointInputStream use compression method");
        }
        result = (ret == JNI_TRUE);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
    return result;
}

void OmniTaskBridgeImpl2::setSavepointInputStreamOffset(jobject inputStream, int64_t offset)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: setSavepointInputStreamOffset could not AttachCurrentThread for JNI call");
        throw std::runtime_error("setSavepointInputStreamOffset could not AttachCurrentThread for JNI call");
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: setSavepointInputStreamOffset could not GetObjectClass for JNI call");
            throw std::runtime_error("setSavepointInputStreamOffset could not GetObjectClass for JNI call");
        }
        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "setSavepointInputStreamOffset",
            "(Lorg/apache/flink/core/fs/FSDataInputStream;J)V");
        if (mid == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: setSavepointInputStreamOffset could not GetMethodID for JNI call");
            throw std::runtime_error("setSavepointInputStreamOffset could not GetMethodID for JNI call");
        }
        env->CallObjectMethod(m_globalOmniTaskRef, mid, inputStream, offset);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: Failed to call SavepointInputStream set offset method");
            throw std::runtime_error("Failed to call SavepointInputStream set offset method");
        }
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
        throw std::runtime_error("Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

void OmniTaskBridgeImpl2::closeSavepointInputStream(jobject inputStream)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: closeSavepointInputStream could not AttachCurrentThread for JNI call");
        throw std::runtime_error("closeSavepointInputStream could not AttachCurrentThread for JNI call");
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: closeSavepointInputStream omniTaskWrapperClass == nullptr");
            throw std::runtime_error("closeSavepointInputStream omniTaskWrapperClass == nullptr");
        }
        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "closeSavepointInputStream",
            "(Lorg/apache/flink/core/fs/FSDataInputStream;)V");
        if (mid == nullptr) {
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: closeSavepointInputStream could not GetMethodID for JNI call");
            throw std::runtime_error("closeSavepointInputStream could not GetMethodID for JNI call");
        }
        env->CallObjectMethod(m_globalOmniTaskRef, mid, inputStream);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ExceptionClear();
            env->DeleteLocalRef(inputStream);
            g_OmniStreamJVM->DetachCurrentThread();
            INFO_RELEASE("Error: Failed to call SavepointInputStream close method");
            throw std::runtime_error("Failed to call SavepointInputStream close method");
        }
        env->DeleteLocalRef(omniTaskWrapperClass);
        env->DeleteLocalRef(inputStream);
    } else {
        INFO_RELEASE("Error: Could not get TaskStateManagerWrapper class for JNI call");
        throw std::runtime_error("Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

jobject OmniTaskBridgeImpl2::AcquireSavepointOutputStream(long checkpointId, CheckpointOptions *checkpointOptions)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside AcquireSavepointOutputStream");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside AcquireSavepointOutputStream");
    }
    if (checkpointOptions == nullptr) {
        INFO_RELEASE("Error: checkpointOptions is nullptr in TaskStateManagerBridgeImpl::AcquireSavepointOutputStream");
        throw std::runtime_error("checkpointOptions is nullptr in TaskStateManagerBridgeImpl::AcquireSavepointOutputStream");
    }
    nlohmann::json jcheckpointOptions = checkpointOptions->ToJson();
    std::string checkpointOptionsStr = jcheckpointOptions.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "acquireSavepointOutputStream", "(JLjava/lang/String;)Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;");
        jstring jcheckpointOptionsStr = env->NewStringUTF(checkpointOptionsStr.c_str());
    auto localProvider =  env->CallObjectMethod(m_globalOmniTaskRef, mid, checkpointId, jcheckpointOptionsStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jcheckpointOptionsStr);
        INFO_RELEASE("Error: Failed to call AcquireSavepointOutputStream");
        throw std::runtime_error("Failed to call AcquireSavepointOutputStream");
    }
    env->DeleteLocalRef(jcheckpointOptionsStr);
    if (localProvider == nullptr) {
        return nullptr;
    }
    // CallObjectMethod 返回 local ref，仅在当前 JNI frame、当前线程有效。
    // CheckpointStateOutputStreamProxy 会把 provider 跨线程（async checkpoint thread）
    // 持续使用，必须升级为 global ref；CloseSavepointOutputStream 中 DeleteGlobalRef 释放。
    jobject globalProvider = env->NewGlobalRef(localProvider);
    env->DeleteLocalRef(localProvider);
    return globalProvider;
}

std::shared_ptr<SnapshotResult<StreamStateHandle>> OmniTaskBridgeImpl2::CloseSavepointOutputStream(jobject provider)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside CloseSavepointOutputStream");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside CloseSavepointOutputStream");
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "closeSavepointOutputStream", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;)Lorg/apache/flink/runtime/state/SnapshotResult;");
    jobject javaResult = env->CallObjectMethod(m_globalOmniTaskRef, mid, provider);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteGlobalRef(provider);
        INFO_RELEASE("Error: Failed to call CloseSavepointOutputStream");
        throw std::runtime_error("Failed to call CloseSavepointOutputStream");
    }
    auto res =  helper.ConvertSnapshotResult(env, javaResult);
    env->DeleteGlobalRef(provider);
    return res;
}

void OmniTaskBridgeImpl2::WriteSavepointOutputStream(jobject provider, const int8_t *chunk, size_t offset, size_t len)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside WriteSavepointOutputStream");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside WriteSavepointOutputStream");
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "writeSavepointOutputStream", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;[B)V");
    jbyteArray data = env->NewByteArray(len);
    env->SetByteArrayRegion(data, offset, len, chunk);
    env->CallVoidMethod(m_globalOmniTaskRef, mid, provider, data);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(data);
        INFO_RELEASE("Error: Failed to call WriteSavepointOutputStream");
        throw std::runtime_error("Failed to call WriteSavepointOutputStream");
    }
    env->DeleteLocalRef(data);
}

void OmniTaskBridgeImpl2::WriteSavepointMetadata(jobject provider, const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
                                                 std::string keySerializer)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside WriteSavepointMetadata");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside WriteSavepointMetadata");
    }

    nlohmann::json stateMetaInfoJson = nlohmann::json::array();
    //TODO 需要增加key序列化器和namespace序列化器以保证omnistream创建的savepoint在flink可恢复
    for (const auto& snapshot : snapshots) {
        nlohmann::json jsonObj;
        jsonObj["name"] = snapshot->getName();
        jsonObj["backendStateType"] =
        static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj["options"] = snapshot->getOptionsImmutable();
        jsonObj["serializer"] = snapshot->getSerializerJson();
        jsonObj["keySerializer"] = keySerializer;
        stateMetaInfoJson.push_back(std::move(jsonObj));
    }
    std::string stateMetaInfoStr = stateMetaInfoJson.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "writeSavepointMetadata", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;Ljava/lang/String;)V");
    jstring jStateMetaInfoStr = env->NewStringUTF(stateMetaInfoStr.c_str());
    env->CallVoidMethod(m_globalOmniTaskRef, mid, provider, jStateMetaInfoStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jStateMetaInfoStr);
        INFO_RELEASE("Error: Failed to call WriteSavepointMetadata");
        throw std::runtime_error("Failed to call WriteSavepointMetadata");
    }
    env->DeleteLocalRef(jStateMetaInfoStr);
}

void OmniTaskBridgeImpl2::WriteOperatorMetaData(
    jobject provider,
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& operatorStateMetaInfoSnapshots_,
    const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& broadcastStateMetaInfoSnapshots_) {

    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside WriteSavepointMetadata");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside WriteSavepointMetadata");
    }

    nlohmann::json operatorStateMetaInfoJson = nlohmann::json::array();
    nlohmann::json broadcastStateMetaInfoJson = nlohmann::json::array();
    for (const auto& snapshot : operatorStateMetaInfoSnapshots_) {
        try {
            if (snapshot == nullptr) {
                continue;
            }
            nlohmann::json jsonObj;
            jsonObj["name"] = snapshot->getName();
            jsonObj["backendStateType"] = static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
            jsonObj["options"] = snapshot->getOptionsImmutable();
            jsonObj["serializer"] = snapshot->getSerializerJson();
            operatorStateMetaInfoJson.push_back(std::move(jsonObj));
        } catch (const std::exception& e) {
            INFO_RELEASE("OmniTaskBridgeImpl2::WriteOperatorMetaData error " + std::string(e.what()));
                    throw std::runtime_error("Failed to WriteOperatorMetaData");
        }
    }
    for (const auto& snapshot : broadcastStateMetaInfoSnapshots_) {
        if (snapshot == nullptr) {
            continue;
        }
        nlohmann::json jsonObj;
        jsonObj["name"] = snapshot->getName();
        jsonObj["backendStateType"] = static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj["options"] = snapshot->getOptionsImmutable();
        jsonObj["serializer"] = snapshot->getSerializerJson();
        broadcastStateMetaInfoJson.push_back(std::move(jsonObj));
    }
    std::string operatorStateMetaInfoStr = operatorStateMetaInfoJson.dump();
    std::string broadcastStateMetaInfoStr = broadcastStateMetaInfoJson.dump();

    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "writeOperatorMetaData",
        "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;Ljava/lang/String;Ljava/lang/String;)V"
    );

    jstring jOperatorStateMetaInfoStr = env->NewStringUTF(operatorStateMetaInfoStr.c_str());
    jstring jBroadcastStateMetaInfoStr = env->NewStringUTF(broadcastStateMetaInfoStr.c_str());
    env->CallObjectMethod(m_globalOmniTaskRef, mid, provider, jOperatorStateMetaInfoStr, jBroadcastStateMetaInfoStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jOperatorStateMetaInfoStr);
        env->DeleteLocalRef(jBroadcastStateMetaInfoStr);
        INFO_RELEASE("Error: Failed to call WriteOperatorMetaData");
        throw std::runtime_error("Failed to call WriteOperatorMetaData");
    }
    env->DeleteLocalRef(jOperatorStateMetaInfoStr);
    env->DeleteLocalRef(jBroadcastStateMetaInfoStr);
}

long OmniTaskBridgeImpl2::GetSavepointOutputStreamPos(jobject provider)
{
    JNIEnv* env = nullptr;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    jint attachRes = 0;
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void **>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        INFO_RELEASE("Error: Failed to attach C++ thread to JVM inside GetSavepointOutputStreamPos");
        throw std::runtime_error("Failed to attach C++ thread to JVM inside GetSavepointOutputStreamPos");
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "getSavepointOutputStreamPos", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;)J");
    auto pos = env->CallLongMethod(m_globalOmniTaskRef, mid, provider);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        //env->DeleteLocalRef(provider);
        INFO_RELEASE("Error: Failed to call GetSavepointOutputStreamPos");
        throw std::runtime_error("Failed to call GetSavepointOutputStreamPos");
    }
    return pos;
}

JNIEnv* OmniTaskBridgeImpl2::getJNIEnv()
{
    JNIEnv* env = nullptr;
    jint attachRes = 0;
    jint ret = g_OmniStreamJVM->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8);
    if (ret == JNI_EDETACHED) {
        attachRes = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    }
    if (attachRes != JNI_OK || env == nullptr) {
        GErrorLog("Failed to attach C++ thread to JVM inside CallUploadFilesToCheckpointFs");
        return nullptr;
    }
    return env;
}
