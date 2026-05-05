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

#ifndef OMNISTREAM_OMNITASKBRIDGEHELPER_H
#define OMNISTREAM_OMNITASKBRIDGEHELPER_H

#include <vector>
#include <cstdint>
#include <jni.h>

#include <utils/exception/Throwable.h>
#include "state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/OperatorStateHandle.h"
#include "runtime/state/restore/KeyGroupEntry.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include "core/fs/Path.h"
#include "typeinfo/TypeInfoFactory.h"
#include "state/filesystem/FileStateHandle.h"
#include "state/memory/ByteStreamStateHandle.h"
#include "state/filesystem/RelativeFileStateHandle.h"
#include "runtime/state/OperatorStreamStateHandle.h"

class OmniTaskBridgeHelper {
public:
    enum class StreamStateHandleType {
        Unknown,
        ByteStreamStateHandle,
        RelativeFileStateHandle,
        FileStateHandle,
        OperatorStreamStateHandle,
    };

    std::vector<int8_t> jbyteArrayToVector(JNIEnv* env, jbyteArray byteArray);

    std::string JstringToString(JNIEnv* env, jstring jstr);

    std::string FlinkPathToString(JNIEnv* env, jobject flinkPathObj);

    std::unordered_map<std::string, OperatorStateHandle::StateMetaInfo> OperatorPartitionOffsetsToMap(JNIEnv* env, jobject jMap);

    std::shared_ptr<StreamStateHandle> CreateFileStateHandle(JNIEnv* env, jobject handleObj);

    std::shared_ptr<StreamStateHandle> CreateRelativeFileStateHandle(JNIEnv* env, jobject handleObj);

    std::shared_ptr<StreamStateHandle> CreateByteStreamStateHandle(JNIEnv* env, jobject handleObj);

    std::shared_ptr<StreamStateHandle> CreateOperatorStreamStateHandle(JNIEnv* env, jobject handleObj);

    OmniTaskBridgeHelper::StreamStateHandleType GetHandleType(JNIEnv* env, jobject handleObj);

    std::shared_ptr<StreamStateHandle> GetStreamStateHandle(JNIEnv* env, jobject handleObj);

    std::shared_ptr<SnapshotResult<StreamStateHandle>> ConvertSnapshotResult(JNIEnv* env, jobject jSnapshotResult);


    jobject ConvertToJavaByteStreamStateHandle(JNIEnv* env, const ByteStreamStateHandle& cppHandle);

    jobject ConvertToJavaFileStateHandle(JNIEnv* env, const FileStateHandle& cppHandle);

    jobject ConvertToJavaRelativeFileStateHandle(JNIEnv* env, const RelativeFileStateHandle& cppHandle);

    jobject ConvertPathStrToJavaPath(JNIEnv* env, const std::filesystem::path& restoreInstancePath);

    jobject ConvertToJavaStreamStateHandle(JNIEnv* env, const StreamStateHandle& cppHandle);

    std::vector<StateMetaInfoSnapshot> convertResult(const std::string& cppResult);
};

#endif //OMNISTREAM_OMNITASKBRIDGEHELPER_H
