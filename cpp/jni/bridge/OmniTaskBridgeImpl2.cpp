/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 */

#include "OmniTaskBridgeImpl2.h"
#include "state/filesystem/FileStateHandle.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "state/filesystem/RelativeFileStateHandle.h"
#include "typeinfo/TypeInfoFactory.h"

enum class StreamStateHandleType {
    Unknown,
    ByteStreamStateHandle,
    RelativeFileStateHandle,
    FileStateHandle
};

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

std::string JstringToString(JNIEnv* env, jstring jstr)
{
    if (!env || !jstr) {
        return "";
    }

    const char* cStr = env->GetStringUTFChars(jstr, nullptr);
    if (!cStr) {
        env->ExceptionClear();
        return "";
    }

    std::string result(cStr);
    env->ReleaseStringUTFChars(jstr, cStr);
    return result;
}

std::string FlinkPathToString(JNIEnv* env, jobject flinkPathObj)
{
    if (!env || !flinkPathObj) {
        return "";
    }

    const char* pathClassPath = "org/apache/flink/core/fs/Path";
    jclass pathClass = env->FindClass(pathClassPath);
    if (!pathClass) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        throw std::runtime_error("Failed to find org.apache.flink.core.fs.Path class");
    }

    jmethodID getPathMethod = env->GetMethodID(pathClass, "getPath", "()Ljava/lang/String;");
    if (!getPathMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(pathClass);
        throw std::runtime_error("Failed to get Path.getPath() method ID");
    }

    jstring pathStr = static_cast<jstring>(env->CallObjectMethod(flinkPathObj, getPathMethod));

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(pathClass);
        throw std::runtime_error("Failed to call Path.getPath() method");
    }

    std::string result = JstringToString(env, pathStr);

    env->DeleteLocalRef(pathStr);
    env->DeleteLocalRef(pathClass);

    return result;
}


std::shared_ptr<StreamStateHandle> CreateFileStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get FileStateHandle class");
    }

    jmethodID getFilePathMethod = env->GetMethodID(handleClass, "getFilePath", "()Lorg/apache/flink/core/fs/Path;");
    jmethodID getStateSizeMethod = env->GetMethodID(handleClass, "getStateSize", "()J");
    if (!getFilePathMethod || !getStateSizeMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get FileStateHandle method IDs");
    }

    jobject jFilePath = env->CallObjectMethod(handleObj, getFilePathMethod);
    jlong jStateSize = env->CallLongMethod(handleObj, getStateSizeMethod);

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jFilePath) env->DeleteLocalRef(jFilePath);
        throw std::runtime_error("Failed to call FileStateHandle get methods");
    }

    std::string filePathStr = FlinkPathToString(env, jFilePath);
    Path filePath(filePathStr);
    uint64_t stateSize = static_cast<uint64_t>(jStateSize);

    env->DeleteLocalRef(jFilePath);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<FileStateHandle>(filePath, stateSize);
}

std::shared_ptr<StreamStateHandle> CreateRelativeFileStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get RelativeFileStateHandle class");
    }

    jmethodID getFilePathMethod = env->GetMethodID(
        handleClass,
        "getFilePath",
        "()Lorg/apache/flink/core/fs/Path;"
    );
    jmethodID getRelativePathMethod = env->GetMethodID(handleClass, "getRelativePath", "()Ljava/lang/String;");
    jmethodID getStateSizeMethod = env->GetMethodID(handleClass, "getStateSize", "()J");
    if (!getFilePathMethod || !getRelativePathMethod || !getStateSizeMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get RelativeFileStateHandle method IDs");
    }

    jobject jFilePath = env->CallObjectMethod(handleObj, getFilePathMethod);
    jobject jRelativePath = env->CallObjectMethod(handleObj, getRelativePathMethod);
    jlong jStateSize = env->CallLongMethod(handleObj, getStateSizeMethod);

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jFilePath) env->DeleteLocalRef(jFilePath);
        if (jRelativePath) env->DeleteLocalRef(jRelativePath);
        throw std::runtime_error("Failed to call RelativeFileStateHandle get methods");
    }

    std::string filePathStr = FlinkPathToString(env, jFilePath);
    Path filePath(filePathStr);
    std::string relativePath = JstringToString(env, static_cast<jstring>(jRelativePath));
    uint64_t stateSize = static_cast<uint64_t>(jStateSize);

    env->DeleteLocalRef(jFilePath);
    env->DeleteLocalRef(jRelativePath);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<RelativeFileStateHandle>(filePath, relativePath, stateSize);
}


std::shared_ptr<StreamStateHandle> CreateByteStreamStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get ByteStreamStateHandle class");
    }

    jmethodID getHandleNameMethod = env->GetMethodID(handleClass, "getHandleName", "()Ljava/lang/String;");
    jmethodID getDataMethod = env->GetMethodID(handleClass, "getData", "()[B");
    if (!getHandleNameMethod || !getDataMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get ByteStreamStateHandle method IDs");
    }

    jobject jHandleName = env->CallObjectMethod(handleObj, getHandleNameMethod);
    jbyteArray jData = static_cast<jbyteArray>(env->CallObjectMethod(handleObj, getDataMethod));

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jHandleName) env->DeleteLocalRef(jHandleName);
        if (jData) env->DeleteLocalRef(jData);
        throw std::runtime_error("Failed to call ByteStreamStateHandle get methods");
    }

    std::string handleName = JstringToString(env, static_cast<jstring>(jHandleName));
    std::vector<uint8_t> data;

    if (jData) {
        jsize dataLen = env->GetArrayLength(jData);
        jbyte* dataBytes = env->GetByteArrayElements(jData, nullptr);

        if (dataBytes) {
            data.assign(reinterpret_cast<uint8_t*>(dataBytes),
                        reinterpret_cast<uint8_t*>(dataBytes + dataLen));
            env->ReleaseByteArrayElements(jData, dataBytes, 0);
        }
        env->DeleteLocalRef(jData);
    }

    env->DeleteLocalRef(jHandleName);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<ByteStreamStateHandle>(handleName, data);
}

StreamStateHandleType GetHandleType(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        return StreamStateHandleType::Unknown;
    }

    const char* byteStreamClassPath = "org/apache/flink/runtime/state/memory/ByteStreamStateHandle";
    const char* relativeFileClassPath = "org/apache/flink/runtime/state/filesystem/RelativeFileStateHandle";
    const char* fileClassPath = "org/apache/flink/runtime/state/filesystem/FileStateHandle";

    jclass byteStreamClass = env->FindClass(byteStreamClassPath);
    jclass relativeFileClass = env->FindClass(relativeFileClassPath);
    jclass fileClass = env->FindClass(fileClassPath);

    bool isByteStream = false;
    bool isRelativeFile = false;
    bool isFile = false;

    if (byteStreamClass) {
        isByteStream = env->IsInstanceOf(handleObj, byteStreamClass);
        env->DeleteLocalRef(byteStreamClass);
    }
    if (relativeFileClass) {
        isRelativeFile = env->IsInstanceOf(handleObj, relativeFileClass);
        env->DeleteLocalRef(relativeFileClass);
    }
    if (fileClass) {
        isFile = env->IsInstanceOf(handleObj, fileClass);
        env->DeleteLocalRef(fileClass);
    }

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        return StreamStateHandleType::Unknown;
    }

    if (isByteStream) {
        return StreamStateHandleType::ByteStreamStateHandle;
    } else if (isRelativeFile) {
        return StreamStateHandleType::RelativeFileStateHandle;
    } else if (isFile) {
        return StreamStateHandleType::FileStateHandle;
    } else {
        return StreamStateHandleType::Unknown;
    }
}

std::shared_ptr<StreamStateHandle> GetStreamStateHandle(JNIEnv* env, jobject jHandle)
{
    std::shared_ptr<StreamStateHandle> handle;

    if (jHandle) {
        auto type = GetHandleType(env, jHandle);
        switch (type) {
            case StreamStateHandleType::FileStateHandle:
                handle = CreateFileStateHandle(env, jHandle);
                break;
            case StreamStateHandleType::RelativeFileStateHandle:
                handle = CreateRelativeFileStateHandle(env, jHandle);
                break;
            case StreamStateHandleType::ByteStreamStateHandle:
                handle = CreateByteStreamStateHandle(env, jHandle);
                break;
            default:
                handle = nullptr;
        }
        env->DeleteLocalRef(jHandle);
    }
    return handle;
}

std::shared_ptr<SnapshotResult<StreamStateHandle>> ConvertSnapshotResult(JNIEnv* env, jobject jSnapshotResult)
{
    jclass snapshotResultClass = env->FindClass("org/apache/flink/runtime/state/SnapshotResult");
    if (snapshotResultClass == nullptr) {
        return nullptr;
    }

    jmethodID midGetJobManager = env->GetMethodID(
        snapshotResultClass,
        "getJobManagerOwnedSnapshot",
        "()Lorg/apache/flink/runtime/state/StateObject;"
    );
    if (midGetJobManager == nullptr) {
        env->DeleteLocalRef(snapshotResultClass);
        return nullptr;
    }

    jmethodID midGetTaskLocal = env->GetMethodID(
        snapshotResultClass,
        "getTaskLocalSnapshot",
        "()Lorg/apache/flink/runtime/state/StateObject;"
    );
    if (midGetTaskLocal == nullptr) {
        env->DeleteLocalRef(snapshotResultClass);
        return nullptr;
    }

    jobject jobManagerSnapshotJobj = env->CallObjectMethod(jSnapshotResult, midGetJobManager);
    jobject taskLocalSnapshotJobj = env->CallObjectMethod(jSnapshotResult, midGetTaskLocal);

    auto jobManagerSnapshot = GetStreamStateHandle(env, jobManagerSnapshotJobj);
    auto taskLocalSnapshot = GetStreamStateHandle(env, taskLocalSnapshotJobj);

    env->DeleteLocalRef(snapshotResultClass);

    return SnapshotResult<StreamStateHandle>::WithLocalState(jobManagerSnapshot, taskLocalSnapshot);
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

    return ConvertSnapshotResult(env, resultObj);
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

std::vector<StateMetaInfoSnapshot> convertResult(const std::string& cppResult)
{
    // reconstruct std::vector<StateMetaInfoSnapshot>
    std::vector<StateMetaInfoSnapshot> toReturn;
    nlohmann::json parsed = nlohmann::json::parse(cppResult);
    for (const auto& oneSnapshot : parsed) {
        std::unordered_map<std::string, std::string> tmpOptions;
        if (!oneSnapshot.contains("backendStateType") ||
            !oneSnapshot["backendStateType"].is_string() ||
            !oneSnapshot.contains("name") ||
            !oneSnapshot["name"].is_string() ||
            !oneSnapshot.contains("optionsImmutable")) {
            throw std::runtime_error("snapshot json format invalid.");
        }
        for (const auto& [key, value] : oneSnapshot["optionsImmutable"].items()) {
            tmpOptions[key] = value.get<std::string>();
        }
        std::unordered_map<std::string, TypeSerializer *> tmpSerializers;
        if(oneSnapshot.contains("serializer")){
            auto serializers = oneSnapshot["serializer"];
            if(serializers.contains("namespaceSerializer")){
                auto namespaceSerializer = TypeInfoFactory::createDataStreamTypeInfo(serializers["namespaceSerializer"]);
                if(namespaceSerializer != nullptr){
                    tmpSerializers.emplace("NAMESPACE_SERIALIZER", namespaceSerializer->getTypeSerializer());
                }
            }
            if(serializers.contains("stateSerializer")){
                auto stateSerializer = TypeInfoFactory::createDataStreamTypeInfo(serializers["stateSerializer"]);
                if(stateSerializer != nullptr){
                    tmpSerializers.emplace("VALUE_SERIALIZER", stateSerializer->getTypeSerializer());
                }
            }
        }
        // Currently we don't take snapshot of serializers
        StateMetaInfoSnapshot::BackendStateType bst;
        auto backendStateTypeStr = oneSnapshot["backendStateType"].get<std::string>();
        if (backendStateTypeStr == "KEY_VALUE" || backendStateTypeStr == "PRIORITY_QUEUE") {
            bst = StateMetaInfoSnapshot::BackendStateType::KEY_VALUE;
        } else if (backendStateTypeStr == "OPERATOR" ||
                   backendStateTypeStr == "BROADCAST") {
            LOG("Unsupport BackendStateType.")
            continue;
        } else {
            throw std::runtime_error("Unknown BackendStateType.");
        }
        toReturn.push_back(StateMetaInfoSnapshot(oneSnapshot["name"].get<std::string>(), bst, tmpOptions, {}, tmpSerializers));
    }
    return toReturn;
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

        return convertResult(cppResult);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
        return {};
    }
}

std::vector<int8_t> jbyteArrayToVector(JNIEnv* env, jbyteArray byteArray)
{
    std::vector<int8_t> result;
    if (!byteArray) {
        return result;
    }

    jsize length = env->GetArrayLength(byteArray);
    if (length < 0) {
        return result;
    }

    result.reserve(length);
    jbyte* data = static_cast<jbyte*>(env->GetPrimitiveArrayCritical(byteArray, nullptr));
    if (data != nullptr) {
        result.assign(data, data + length);
        env->ReleasePrimitiveArrayCritical(byteArray, data, JNI_ABORT);
    }
    return result;
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

        jfieldID currentKvStateIdField = env->GetFieldID(entryWrapperClass, "currentKvStateId", "I");
        jfieldID entriesField = env->GetFieldID(entryWrapperClass, "entries", "[Lcom/huawei/omniruntime/flink/runtime/restore/KeyGroupEntry;");
        jfieldID entryKvStateIdField = env->GetFieldID(entryWrapperClass, "kvStateId", "I");
        jfieldID entryCountField = env->GetFieldID(entryWrapperClass, "count", "I");

        jfieldID entryKeyField = env->GetFieldID(entryClass, "key", "[B");
        jfieldID entryValueField = env->GetFieldID(entryClass, "value", "[B");

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
            return;
        }

        if (result == nullptr) {
            GErrorLog("Error: getKeyGroupEntries get null result for JNI call");
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

            entries.emplace_back(KeyGroupEntry(kvStateId, std::move(jbyteArrayToVector(env, keyArray)),
                std::move(jbyteArrayToVector(env, valueArray))));
            
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
        return nullptr;
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            INFO_RELEASE("Error: getSavepointInputStream could not GetObjectClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return nullptr;
        }
        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "getSavepointInputStream",
            "(Ljava/lang/String;)Lorg/apache/flink/core/fs/FSDataInputStream;");
        if (mid == nullptr) {
            INFO_RELEASE("Error: getSavepointInputStream could not get methodID for JNI call");
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return nullptr;
        }
        jstring msHandle = env->NewStringUTF(metaStateHandle.c_str());
        inputStream = env->CallObjectMethod(m_globalOmniTaskRef, mid, msHandle);
        env->DeleteLocalRef(msHandle);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
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
        return;
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            INFO_RELEASE("Error: setSavepointInputStreamOffset could not GetObjectClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }
        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "setSavepointInputStreamOffset",
            "(Lorg/apache/flink/core/fs/FSDataInputStream;J)V");
        if (mid == nullptr) {
            INFO_RELEASE("Error: setSavepointInputStreamOffset could not GetMethodID for JNI call");
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }
        env->CallObjectMethod(m_globalOmniTaskRef, mid, inputStream, offset);
        env->DeleteLocalRef(omniTaskWrapperClass);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
    }
    g_OmniStreamJVM->DetachCurrentThread();
}

void OmniTaskBridgeImpl2::closeSavepointInputStream(jobject inputStream)
{
    JNIEnv* env;
    jint res = g_OmniStreamJVM->AttachCurrentThread(reinterpret_cast<void**>(&env), nullptr);
    if (res != JNI_OK) {
        INFO_RELEASE("Error: closeSavepointInputStream could not AttachCurrentThread for JNI call");
        return;
    }
    if (m_globalOmniTaskRef != nullptr) {
        jclass omniTaskWrapperClass = env->GetObjectClass(m_globalOmniTaskRef);
        if (omniTaskWrapperClass == nullptr) {
            INFO_RELEASE("Error: closeSavepointInputStream could not GetObjectClass for JNI call");
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }
        jmethodID mid = env->GetMethodID(omniTaskWrapperClass, "closeSavepointInputStream",
            "(Lorg/apache/flink/core/fs/FSDataInputStream;)V");
        if (mid == nullptr) {
            INFO_RELEASE("Error: closeSavepointInputStream could not GetMethodID for JNI call");
            env->DeleteLocalRef(omniTaskWrapperClass); // Clean up local ref
            g_OmniStreamJVM->DetachCurrentThread();
            return;
        }
        env->CallObjectMethod(m_globalOmniTaskRef, mid, inputStream);
        env->DeleteLocalRef(omniTaskWrapperClass);
        env->DeleteLocalRef(inputStream);
    } else {
        GErrorLog("Error: Could not get TaskStateManagerWrapper class for JNI call");
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
        GErrorLog("Failed to attach C++ thread to JVM inside AcquireSavepointOutputStream");
        return nullptr;
    }
    if (checkpointOptions == nullptr) {
        GErrorLog("checkpointOptions is nullptr in TaskStateManagerBridgeImpl::AcquireSavepointOutputStream");
        return nullptr;
    }
    nlohmann::json jcheckpointOptions = checkpointOptions->ToJson();
    std::string checkpointOptionsStr = jcheckpointOptions.dump();
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "acquireSavepointOutputStream", "(JLjava/lang/String;)Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;");
        jstring jcheckpointOptionsStr = env->NewStringUTF(checkpointOptionsStr.c_str());
    auto provider =  env->CallObjectMethod(m_globalOmniTaskRef, mid, checkpointId, jcheckpointOptionsStr);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(jcheckpointOptionsStr);
        throw std::runtime_error("Failed to call AcquireSavepointOutputStream");
    }
    env->DeleteLocalRef(jcheckpointOptionsStr);
    return provider;
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
        GErrorLog("Failed to attach C++ thread to JVM inside CloseSavepointOutputStream");
        return nullptr;
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "closeSavepointOutputStream", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;)Lorg/apache/flink/runtime/state/SnapshotResult;");
    jobject javaResult = env->CallObjectMethod(m_globalOmniTaskRef, mid, provider);
    auto res =  ConvertSnapshotResult(env, javaResult);
    env->DeleteLocalRef(provider);
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
        GErrorLog("Failed to attach C++ thread to JVM inside WriteSavepointOutputStream");
        return;
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "writeSavepointOutputStream", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;[B)V");
    jbyteArray data = env->NewByteArray(len);
    env->SetByteArrayRegion(data, offset, len, chunk);
    env->CallVoidMethod(m_globalOmniTaskRef, mid, provider, data);
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
        GErrorLog("Failed to attach C++ thread to JVM inside WriteSavepointMetadata");
        return;
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
    env->DeleteLocalRef(jStateMetaInfoStr);
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
        GErrorLog("Failed to attach C++ thread to JVM inside GetSavepointOutputStreamPos");
        return -1;
    }
    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(cls, "getSavepointOutputStreamPos", "(Lorg/apache/flink/runtime/state/CheckpointStreamWithResultProvider;)J");
    return env->CallLongMethod(m_globalOmniTaskRef, mid, provider);
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
