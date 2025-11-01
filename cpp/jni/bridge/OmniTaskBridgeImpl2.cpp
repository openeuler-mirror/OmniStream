/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 */

#include "OmniTaskBridgeImpl2.h"
#include "state/filesystem/FileStateHandle.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "state/filesystem/RelativeFileStateHandle.h"

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
    std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots)
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

    nlohmann::json stateMetaInfoJson = nlohmann::json::array();
    for (const auto& snapshot : snapshots) {
        nlohmann::json jsonObj;
        jsonObj["name"] = snapshot->getName();
        jsonObj["backendStateType"] =
        static_cast<int>(StateMetaInfoSnapshot::getCode(snapshot->getBackendStateType()));
        jsonObj["options"] = snapshot->getOptionsImmutable();
        stateMetaInfoJson.push_back(std::move(jsonObj));
    }
    std::string stateMetaInfoStr = stateMetaInfoJson.dump();

    jclass cls = env->GetObjectClass(m_globalOmniTaskRef);
    jmethodID mid = env->GetMethodID(
        cls,
        "materializeMetaData",
        "(JLjava/lang/String;)Lorg/apache/flink/runtime/state/SnapshotResult;"
    );
    jstring jStateMetaInfoStr = env->NewStringUTF(stateMetaInfoStr.c_str());
    jobject resultObj = env->CallObjectMethod(m_globalOmniTaskRef, mid, checkpointId, jStateMetaInfoStr);

    env->DeleteLocalRef(jStateMetaInfoStr);
    env->DeleteLocalRef(cls);

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
        toReturn.push_back(StateMetaInfoSnapshot(oneSnapshot["name"].get<std::string>(), bst, tmpOptions, {}, {}));
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
