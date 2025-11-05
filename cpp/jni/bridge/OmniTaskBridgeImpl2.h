/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 * Create Date : 2025
 *
 * OmniTaskBridgeImpl2
 */

#ifndef OMNITASKBRIDGEIMPL2_H
#define OMNITASKBRIDGEIMPL2_H

#include <common.h>
#include <jni.h>
#include <state/bridge/OmniTaskBridge.h>
#include <state/metainfo/StateMetaInfoSnapshot.h>
#include <common/global.h>
#include "core/fs/Path.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"

class OmniTaskBridgeImpl2 : public omnistream::OmniTaskBridge {
public:
    explicit OmniTaskBridgeImpl2(jobject mGlobalOmniTaskRef): m_globalOmniTaskRef(mGlobalOmniTaskRef) {}

    virtual ~OmniTaskBridgeImpl2();

    void declineCheckpoint(std::string &checkpointIDJson, std::string &failure_reasonJson,
        std::string &exceptionJson) override;

    std::vector<StateMetaInfoSnapshot> readMetaData(const std::string &metaStateHandle) override;

    std::shared_ptr<SnapshotResult<StreamStateHandle>> CallMaterializeMetaData(
            jlong checkpointId,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots) override;

    jobject CallUploadFilesToCheckpointFs(const std::vector<Path>& filePaths,
                                          int numberOfSnapshottingThreads) override;

    JNIEnv* getJNIEnv() override;

public:
    jobject m_globalOmniTaskRef;
};
#endif // OMNITASKBRIDGEIMPL2_H
