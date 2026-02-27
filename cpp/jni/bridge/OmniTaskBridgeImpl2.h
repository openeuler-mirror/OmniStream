/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2025. All rights reserved.
 * Create Date : 2025
 *
 * OmniTaskBridgeImpl2
 */

#ifndef OMNITASKBRIDGEIMPL2_H
#define OMNITASKBRIDGEIMPL2_H

#include <common.h>
#include <memory>
#include <jni.h>
#include <state/bridge/OmniTaskBridge.h>
#include <state/metainfo/StateMetaInfoSnapshot.h>
#include <common/global.h>
#include "core/fs/Path.h"
#include "runtime/state/SnapshotResult.h"
#include "runtime/state/StreamStateHandle.h"
#include "runtime/state/restore/KeyGroupEntry.h"
#include "state/LocalRecoveryConfig.h"
#include "runtime/checkpoint/CheckpointOptions.h"

class OmniTaskBridgeImpl2 : public omnistream::OmniTaskBridge {
public:
    explicit OmniTaskBridgeImpl2(jobject mGlobalOmniTaskRef): m_globalOmniTaskRef(mGlobalOmniTaskRef) {}

    virtual ~OmniTaskBridgeImpl2();

    void declineCheckpoint(std::string &checkpointIDJson, std::string &failure_reasonJson,
        std::string &exceptionJson) override;

    std::vector<StateMetaInfoSnapshot> readMetaData(const std::string &metaStateHandle) override;

    void getKeyGroupEntries(jobject inputStream,
        int &currentKvStateId, bool isUsingKeyGroupCompression, std::vector<KeyGroupEntry> &entries) override;

    jobject getSavepointInputStream(const std::string &metaStateHandle) override;

    void setSavepointInputStreamOffset(jobject inputStream, int64_t offset) override;

    bool isUsingKeyGroupCompression(jobject inputStream) override;

    void closeSavepointInputStream(jobject inputStream) override;

    std::shared_ptr<SnapshotResult<StreamStateHandle>> CallMaterializeMetaData(
            jlong checkpointId,
            std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
            std::shared_ptr<LocalRecoveryConfig> localRecoveryConfig,
            CheckpointOptions *checkpointOptions, std::string keySerializer) override;

    jobject CallUploadFilesToCheckpointFs(const std::vector<Path>& filePaths,
                                          int numberOfSnapshottingThreads) override;

    JNIEnv* getJNIEnv() override;

    jobject AcquireSavepointOutputStream(long checkpointId, CheckpointOptions *checkpointOptions) override;
    std::shared_ptr<SnapshotResult<StreamStateHandle>> CloseSavepointOutputStream(jobject provider) override;
    void WriteSavepointOutputStream(jobject provider, const int8_t *chunk, size_t offset, size_t len) override;
    void WriteSavepointMetadata(jobject provider, const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots,
                                std::string keySerializer) override;
    long GetSavepointOutputStreamPos(jobject provider) override;

public:
    jobject m_globalOmniTaskRef;
};
#endif // OMNITASKBRIDGEIMPL2_H
