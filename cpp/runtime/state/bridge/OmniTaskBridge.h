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
#ifndef OMNITASKBRIDGE_H
#define OMNITASKBRIDGE_H
#include <util/omni_exception.h>
#include <utils/exception/Throwable.h>
#include <vector>
#include <jni.h>
#include "state/metainfo/StateMetaInfoSnapshot.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "core/fs/Path.h"

namespace omnistream {
    class OmniTaskBridge {
    public:
        virtual ~OmniTaskBridge() = default;

        virtual void declineCheckpoint(std::string &checkpointIDJson, std::string &failure_reasonJson, std::string &exceptionJson)=0;

        virtual std::shared_ptr<SnapshotResult<StreamStateHandle>> CallMaterializeMetaData(
                jlong checkpointId,
                std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots) = 0;
        
        virtual jobject CallUploadFilesToCheckpointFs(const std::vector<Path>& filePaths,
                                                      int numberOfSnapshottingThreads) = 0;

        virtual std::vector<StateMetaInfoSnapshot> readMetaData(const std::string &metaStateHandle) = 0;

        virtual JNIEnv* getJNIEnv() = 0;
    };
}

#endif // OMNITASKBRIDGE_H
