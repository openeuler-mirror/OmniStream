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

#include "OmniTaskBridgeHelper.h"

std::vector<int8_t> OmniTaskBridgeHelper::jbyteArrayToVector(JNIEnv* env, jbyteArray byteArray)
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

std::string OmniTaskBridgeHelper::JstringToString(JNIEnv* env, jstring jstr)
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

std::string OmniTaskBridgeHelper::FlinkPathToString(JNIEnv* env, jobject flinkPathObj)
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

std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo> OmniTaskBridgeHelper::OperatorPartitionOffsetsToMap(JNIEnv* env, jobject jMap)
{

    std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo> result;

    if (!env || !jMap) {
        return result;
    }

    jclass mapClass = env->GetObjectClass(jMap);
    if (!mapClass) {
        return result;
    }

    jmethodID entrySetMethod = env->GetMethodID(mapClass, "entrySet", "()Ljava/util/Set;");
    if (!entrySetMethod) {
        env->DeleteLocalRef(mapClass);
        return result;
    }

    jobject entrySet = env->CallObjectMethod(jMap, entrySetMethod);
    if (!entrySet) {
        env->DeleteLocalRef(mapClass);
        return result;
    }

    jclass setClass = env->GetObjectClass(entrySet);
    jmethodID iteratorMethod = env->GetMethodID(setClass, "iterator", "()Ljava/util/Iterator;");
    jobject iterator = env->CallObjectMethod(entrySet, iteratorMethod);

    jclass iteratorClass = env->GetObjectClass(iterator);
    jmethodID hasNextMethod = env->GetMethodID(iteratorClass, "hasNext", "()Z");
    jmethodID nextMethod = env->GetMethodID(iteratorClass, "next", "()Ljava/lang/Object;");

    jclass entryClass = env->FindClass("java/util/Map$Entry");
    jmethodID getKeyMethod = env->GetMethodID(entryClass, "getKey", "()Ljava/lang/Object;");
    jmethodID getValueMethod = env->GetMethodID(entryClass, "getValue", "()Ljava/lang/Object;");

    const char* stateMetaInfoClassPath = "org/apache/flink/runtime/state/OperatorStateHandle$StateMetaInfo";
    jclass stateMetaInfoClass = env->FindClass(stateMetaInfoClassPath);
    jmethodID getOffsetsMethod = env->GetMethodID(stateMetaInfoClass, "getOffsets", "()[J");
    jmethodID getDistributionModeMethod = env->GetMethodID(
        stateMetaInfoClass, "getDistributionMode",
        "()Lorg/apache/flink/runtime/state/OperatorStateHandle$Mode;");

    const char* modeClassPath = "org/apache/flink/runtime/state/OperatorStateHandle$Mode";
    jclass modeClass = env->FindClass(modeClassPath);
    jmethodID nameMethod = env->GetMethodID(modeClass, "name", "()Ljava/lang/String;");

    while (env->CallBooleanMethod(iterator, hasNextMethod)) {
        jobject entry = env->CallObjectMethod(iterator, nextMethod);

        jstring key = static_cast<jstring>(env->CallObjectMethod(entry, getKeyMethod));
        jobject value = env->CallObjectMethod(entry, getValueMethod);

        std::string keyStr = JstringToString(env, key);

        jlongArray offsetsArray = static_cast<jlongArray>(env->CallObjectMethod(value, getOffsetsMethod));
        std::vector<long> offsets;
        if (offsetsArray) {
            jsize len = env->GetArrayLength(offsetsArray);
            jlong* elems = env->GetLongArrayElements(offsetsArray, nullptr);
            if (elems) {
                offsets.assign(elems, elems + len);
                env->ReleaseLongArrayElements(offsetsArray, elems, JNI_ABORT);
            }
            env->DeleteLocalRef(offsetsArray);
        }

        jobject modeObj = env->CallObjectMethod(value, getDistributionModeMethod);
        OperatorStateHandle::Mode distributionMode = OperatorStateHandle::Mode::SPLIT_DISTRIBUTE;
        if (modeObj) {
            jstring modeName = static_cast<jstring>(env->CallObjectMethod(modeObj, nameMethod));
            std::string modeStr = JstringToString(env, modeName);
            if (!modeStr.empty()) {
                distributionMode = OperatorStateHandle::StrToMode(modeStr);
            }
            if (modeName) {
                env->DeleteLocalRef(modeName);
            }
            env->DeleteLocalRef(modeObj);
        }

        result.emplace(keyStr, OperatorStateHandle::StateMetaInfo(offsets, distributionMode));

        if (key) {
            env->DeleteLocalRef(key);
        }
        if (value) {
            env->DeleteLocalRef(value);
        }

        env->DeleteLocalRef(entry);
    }

    env->DeleteLocalRef(iterator);
    env->DeleteLocalRef(iteratorClass);
    env->DeleteLocalRef(entrySet);
    env->DeleteLocalRef(setClass);
    env->DeleteLocalRef(mapClass);
    env->DeleteLocalRef(entryClass);
    env->DeleteLocalRef(stateMetaInfoClass);
    env->DeleteLocalRef(modeClass);

    return result;
}

std::shared_ptr<StreamStateHandle> OmniTaskBridgeHelper::CreateFileStateHandle(JNIEnv* env, jobject handleObj)
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

std::shared_ptr<StreamStateHandle> OmniTaskBridgeHelper::CreateRelativeFileStateHandle(JNIEnv* env, jobject handleObj)
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

std::shared_ptr<StreamStateHandle> OmniTaskBridgeHelper::CreateByteStreamStateHandle(JNIEnv* env, jobject handleObj)
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
    INFO_RELEASE("CreateByteStreamStateHandle | handleName=" << handleName << " | data.size=" << data.size());
    
    env->DeleteLocalRef(jHandleName);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<ByteStreamStateHandle>(handleName, data);
}



std::shared_ptr<StreamStateHandle> OmniTaskBridgeHelper::CreateOperatorStreamStateHandle(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        throw std::invalid_argument("Invalid JNI environment or handle object");
    }

    jclass handleClass = env->GetObjectClass(handleObj);
    if (!handleClass) {
        env->ExceptionDescribe();
        throw std::runtime_error("Failed to get OperatorStreamStateHandle class");
    }

    jmethodID getStateNameToPartitionOffsetsMethod = env->GetMethodID(handleClass, "getStateNameToPartitionOffsets", "()Ljava/util/Map;");
    jmethodID getDelegateStateHandleMethod = env->GetMethodID(handleClass, "getDelegateStateHandle", "()Lorg/apache/flink/runtime/state/StreamStateHandle;");
    if (!getStateNameToPartitionOffsetsMethod || !getDelegateStateHandleMethod) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(handleClass);
        throw std::runtime_error("Failed to get OperatorStreamStateHandle method IDs");
    }

    jobject jStateNameToPartitionOffsets = env->CallObjectMethod(handleObj, getStateNameToPartitionOffsetsMethod);
    jobject jDelegateStateHandle = env->CallObjectMethod(handleObj, getDelegateStateHandleMethod);

    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        env->DeleteLocalRef(handleClass);
        if (jStateNameToPartitionOffsets) env->DeleteLocalRef(jStateNameToPartitionOffsets);
        if (jDelegateStateHandle) env->DeleteLocalRef(jDelegateStateHandle);
        throw std::runtime_error("Failed to call OperatorStreamStateHandle get methods");
    }

    std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo> stateNameToPartitionOffsets;
    std::shared_ptr<StreamStateHandle> delegateStateHandle = nullptr;

    if (jStateNameToPartitionOffsets) {
        stateNameToPartitionOffsets = OperatorPartitionOffsetsToMap(env, jStateNameToPartitionOffsets);
    }
    if (jDelegateStateHandle) {
        delegateStateHandle = GetStreamStateHandle(env, jDelegateStateHandle);
    }

    env->DeleteLocalRef(jStateNameToPartitionOffsets);
    env->DeleteLocalRef(jDelegateStateHandle);
    env->DeleteLocalRef(handleClass);

    return std::make_shared<OperatorStreamStateHandle>(stateNameToPartitionOffsets, delegateStateHandle);
}

OmniTaskBridgeHelper::StreamStateHandleType OmniTaskBridgeHelper::GetHandleType(JNIEnv* env, jobject handleObj)
{
    if (!env || !handleObj) {
        return StreamStateHandleType::Unknown;
    }

    const char* byteStreamClassPath = "org/apache/flink/runtime/state/memory/ByteStreamStateHandle";
    const char* relativeFileClassPath = "org/apache/flink/runtime/state/filesystem/RelativeFileStateHandle";
    const char* fileClassPath = "org/apache/flink/runtime/state/filesystem/FileStateHandle";
    const char* operatorStreamClassPath = "org/apache/flink/runtime/state/OperatorStreamStateHandle";

    jclass byteStreamClass = env->FindClass(byteStreamClassPath);
    jclass relativeFileClass = env->FindClass(relativeFileClassPath);
    jclass fileClass = env->FindClass(fileClassPath);
    jclass operatorStreamClass = env->FindClass(operatorStreamClassPath);

    bool isByteStream = false;
    bool isRelativeFile = false;
    bool isFile = false;
    bool isOperatorStream = false;

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
    if (operatorStreamClass) {
        isOperatorStream = env->IsInstanceOf(handleObj, operatorStreamClass);
        env->DeleteLocalRef(operatorStreamClass);
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
    } else if (isOperatorStream) {
        return StreamStateHandleType::OperatorStreamStateHandle;
    } else {
        return StreamStateHandleType::Unknown;
    }
}

std::shared_ptr<StreamStateHandle> OmniTaskBridgeHelper::GetStreamStateHandle(JNIEnv* env, jobject jHandle)
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
            case StreamStateHandleType::OperatorStreamStateHandle:
                handle = CreateOperatorStreamStateHandle(env, jHandle);
                break;
            default:
                handle = nullptr;
        }
        env->DeleteLocalRef(jHandle);
    }
    return handle;
}

std::shared_ptr<SnapshotResult<StreamStateHandle>> OmniTaskBridgeHelper::ConvertSnapshotResult(JNIEnv* env, jobject jSnapshotResult)
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


jobject OmniTaskBridgeHelper::ConvertToJavaByteStreamStateHandle(JNIEnv* env, const ByteStreamStateHandle& cppHandle)
{
    // 1. 获取 Java 类和方法 ID
    jclass byteStreamStateHandleClass = env->FindClass(
        "org/apache/flink/runtime/state/memory/ByteStreamStateHandle");
    if (!byteStreamStateHandleClass) {
        env->ExceptionDescribe();
        return nullptr;
    }

    jmethodID constructor = env->GetMethodID(
        byteStreamStateHandleClass,
        "<init>",
        "(Ljava/lang/String;[B)V");
    if (!constructor) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(byteStreamStateHandleClass);
        return nullptr;
    }

    // 2. 转换 handleName
    jstring jHandleName = env->NewStringUTF(cppHandle.GetHandleName().c_str());
    if (!jHandleName) {
        env->DeleteLocalRef(byteStreamStateHandleClass);
        return nullptr;
    }

    // 3. 转换 data (std::vector<uint8_t> -> jbyteArray)
    const auto& cppData = cppHandle.GetData();
    jbyteArray jData = env->NewByteArray(static_cast<jsize>(cppData.size()));
    if (!jData) {
        env->DeleteLocalRef(jHandleName);
        env->DeleteLocalRef(byteStreamStateHandleClass);
        return nullptr;
    }
    env->SetByteArrayRegion(
        jData,
        0,
        static_cast<jsize>(cppData.size()),
        reinterpret_cast<const jbyte*>(cppData.data()));

    // 4. 创建 Java 对象
    jobject javaHandle = env->NewObject(
        byteStreamStateHandleClass,
        constructor,
        jHandleName,
        jData);

    // 5. 清理局部引用
    env->DeleteLocalRef(jHandleName);
    env->DeleteLocalRef(jData);
    env->DeleteLocalRef(byteStreamStateHandleClass);

    return javaHandle;
}

jobject OmniTaskBridgeHelper::ConvertToJavaFileStateHandle(JNIEnv* env, const FileStateHandle& cppHandle)
{
    // 1. 获取Java类和方法
    jclass fileStateHandleClass = env->FindClass("org/apache/flink/runtime/state/filesystem/FileStateHandle");
    if (!fileStateHandleClass) {
        env->ExceptionDescribe();
        return nullptr;
    }

    jmethodID constructor = env->GetMethodID(
        fileStateHandleClass,
        "<init>",
        "(Lorg/apache/flink/core/fs/Path;J)V");
    if (!constructor) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(fileStateHandleClass);
        return nullptr;
    }

    // 2. 转换文件路径（Path对象）
    // 先获取Java的Path类
    jclass pathClass = env->FindClass("org/apache/flink/core/fs/Path");
    jmethodID pathConstructor = env->GetMethodID(pathClass, "<init>", "(Ljava/lang/String;)V");

    // 将C++路径转换为Java字符串
    std::string filePathStr = cppHandle.GetFilePath().toString();
    jstring jFilePathStr = env->NewStringUTF(filePathStr.c_str());

    // 创建Java Path对象
    jobject javaPath = env->NewObject(pathClass, pathConstructor, jFilePathStr);

    // 3. 创建Java FileStateHandle对象
    jobject javaHandle = env->NewObject(
        fileStateHandleClass,
        constructor,
        javaPath,
        static_cast<jlong>(cppHandle.GetStateSize())
    );

    // 4. 清理局部引用
    env->DeleteLocalRef(jFilePathStr);
    env->DeleteLocalRef(javaPath);
    env->DeleteLocalRef(pathClass);
    env->DeleteLocalRef(fileStateHandleClass);

    return javaHandle;
}

jobject OmniTaskBridgeHelper::ConvertToJavaRelativeFileStateHandle(JNIEnv* env, const RelativeFileStateHandle& cppHandle)
{
    // 1. 获取Java类和方法
    jclass relativeHandleClass = env->FindClass(
        "org/apache/flink/runtime/state/filesystem/RelativeFileStateHandle");
    if (!relativeHandleClass) {
        env->ExceptionDescribe();
        return nullptr;
    }

    jmethodID constructor = env->GetMethodID(
        relativeHandleClass,
        "<init>",
        "(Lorg/apache/flink/core/fs/Path;Ljava/lang/String;J)V");
    if (!constructor) {
        env->ExceptionDescribe();
        env->DeleteLocalRef(relativeHandleClass);
        return nullptr;
    }

    // 2. 转换文件路径（复用FileStateHandle的Path转换逻辑）
    jclass pathClass = env->FindClass("org/apache/flink/core/fs/Path");
    jmethodID pathConstructor = env->GetMethodID(pathClass, "<init>", "(Ljava/lang/String;)V");

    std::string filePathStr = cppHandle.GetFilePath().toString();
    jstring jFilePathStr = env->NewStringUTF(filePathStr.c_str());
    jobject javaPath = env->NewObject(pathClass, pathConstructor, jFilePathStr);

    // 3. 转换相对路径
    jstring jRelativePath = env->NewStringUTF(cppHandle.GetRelativePath().c_str());

    // 4. 创建Java对象
    jobject javaHandle = env->NewObject(
        relativeHandleClass,
        constructor,
        javaPath,
        jRelativePath,
        static_cast<jlong>(cppHandle.GetStateSize())
    );

    // 5. 清理局部引用
    env->DeleteLocalRef(jFilePathStr);
    env->DeleteLocalRef(javaPath);
    env->DeleteLocalRef(jRelativePath);
    env->DeleteLocalRef(pathClass);
    env->DeleteLocalRef(relativeHandleClass);

    return javaHandle;
}

jobject OmniTaskBridgeHelper::ConvertPathStrToJavaPath(JNIEnv* env, const std::filesystem::path& restoreInstancePath)
{
    // 1. 获取Java的Paths类和get方法
    jclass pathsClass = env->FindClass("java/nio/file/Paths");
    if (pathsClass == nullptr) {
        // 类未找到处理
        return nullptr;
    }

    // 2. 获取Paths.get(String, String...)方法
    jmethodID getMethod = env->GetStaticMethodID(
        pathsClass,
        "get",
        "(Ljava/lang/String;[Ljava/lang/String;)Ljava/nio/file/Path;");
    if (getMethod == nullptr) {
        // 方法未找到处理
        env->DeleteLocalRef(pathsClass);
        return nullptr;
    }

    // 3. 将fs::path转换为jstring
    jstring pathString = env->NewStringUTF(restoreInstancePath.string().c_str());

    // 4. 创建空的String数组作为可变参数（Paths.get的第二个参数）
    jobjectArray emptyArray = env->NewObjectArray(0, env->FindClass("java/lang/String"), nullptr);

    // 5. 调用Paths.get方法
    jobject javaPath = env->CallStaticObjectMethod(
        pathsClass,
        getMethod,
        pathString,
        emptyArray);

    // 6. 清理局部引用
    env->DeleteLocalRef(pathString);
    env->DeleteLocalRef(emptyArray);
    env->DeleteLocalRef(pathsClass);

    return javaPath;
}

jobject OmniTaskBridgeHelper::ConvertToJavaStreamStateHandle(JNIEnv* env, const StreamStateHandle& cppHandle)
{
    // 动态类型检查（如果是 ByteStreamStateHandle）
    if (auto byteHandle = dynamic_cast<const ByteStreamStateHandle*>(&cppHandle)) {
        return ConvertToJavaByteStreamStateHandle(env, *byteHandle);
    } else if (auto fileHandle = dynamic_cast<const FileStateHandle*>(&cppHandle)) {
        return ConvertToJavaFileStateHandle(env, *fileHandle);
    } else if (auto relHandle = dynamic_cast<const RelativeFileStateHandle*>(&cppHandle)) {
        return ConvertToJavaRelativeFileStateHandle(env, *relHandle);
    } else {
        env->ThrowNew(
            env->FindClass("java/lang/UnsupportedOperationException"),
            "Unsupported StreamStateHandle type");
        return nullptr;
    }
}


std::vector<StateMetaInfoSnapshot> OmniTaskBridgeHelper::convertResult(const std::string& cppResult)
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
        if (backendStateTypeStr == "KEY_VALUE") {
            bst = StateMetaInfoSnapshot::BackendStateType::KEY_VALUE;
        } else if (backendStateTypeStr == "OPERATOR") {
            bst = StateMetaInfoSnapshot::BackendStateType::OPERATOR;
        }  else if (backendStateTypeStr == "PRIORITY_QUEUE") {
            bst = StateMetaInfoSnapshot::BackendStateType::PRIORITY_QUEUE;
        } else if (backendStateTypeStr == "BROADCAST") {
            LOG("Unsupport BackendStateType.");
            continue;
        } else {
            throw std::runtime_error("Unknown BackendStateType.");
        }
        toReturn.push_back(StateMetaInfoSnapshot(oneSnapshot["name"].get<std::string>(), bst, tmpOptions, {}, tmpSerializers));
    }
    return toReturn;
}
