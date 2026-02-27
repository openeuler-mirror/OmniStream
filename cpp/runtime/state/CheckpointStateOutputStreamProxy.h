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
#ifndef OMNISTREAM_CHECKPOINTSTATEOUTPUTSTREAMPROXY_H
#define OMNISTREAM_CHECKPOINTSTATEOUTPUTSTREAMPROXY_H
#include <securec.h>
#include <memory>
#include "bridge/OmniTaskBridge.h"
#include "state/SnapshotResult.h"
#include "state/StreamStateHandle.h"
#include "state/bridge/OmniTaskBridge.h"
#include "runtime/checkpoint/CheckpointOptions.h"
#include <jni.h>
#include <stdexcept>

class CheckpointStateOutputStreamProxy {
private:
    jobject provider_;
    std::shared_ptr<omnistream::OmniTaskBridge> bridge_;
    int8_t chunk_[4096];
    size_t offset_ = 0;
    size_t pos_ = 0;

public:
    CheckpointStateOutputStreamProxy(const std::shared_ptr<omnistream::OmniTaskBridge> &bridge, long checkpointId, CheckpointOptions *checkpointOptions): bridge_(bridge)
    {
        provider_ = bridge_->AcquireSavepointOutputStream(checkpointId, checkpointOptions);
        if(!provider_){
            throw std::runtime_error("Failed to AcquireSavepointOutputStream");
        }
    }

    virtual ~CheckpointStateOutputStreamProxy() = default;

    std::shared_ptr<SnapshotResult<StreamStateHandle>> close()
    {
        flush();
        if (provider_ != nullptr) {
            auto res = bridge_->CloseSavepointOutputStream(provider_);
            provider_ = nullptr;
            return res;
        }
        return nullptr;
    }

    void writeMetadata(
        const std::vector<std::shared_ptr<StateMetaInfoSnapshot>>& snapshots, std::string keySerializer)
    {
        if (provider_ == nullptr) {
            return;
        }
        bridge_->WriteSavepointMetadata(provider_, snapshots, keySerializer);
        pos_ = bridge_->GetSavepointOutputStreamPos(provider_);
    }

    void flush()
    {
        if (!provider_) {
            return;
        }
        bridge_->WriteSavepointOutputStream(provider_, chunk_, 0, offset_);
        offset_ = 0;
    }

    void writeShort(int16_t data)
    {
        int8_t bytes[2];
        bytes[0] = static_cast<int8_t>((data >> 8) & 0xFF);
        bytes[1] = static_cast<int8_t>(data & 0xFF);
        writeBytes(bytes, sizeof(bytes));
    }

    void writeInt(int32_t data)
    {
        int8_t bytes[4];
        bytes[0] = static_cast<int8_t>((data >> 24) & 0xFF);
        bytes[1] = static_cast<int8_t>((data >> 16) & 0xFF);
        bytes[2] = static_cast<int8_t>((data >> 8) & 0xFF);
        bytes[3] = static_cast<int8_t>(data & 0xFF);
        writeBytes(bytes, sizeof(bytes));
    }

    void writeBytes(const void *data, size_t len)
    {
        size_t ori_len = len;
        if (!provider_) {
            return;
        }
        const int8_t *src = (const int8_t *)data;
        while (len + offset_ >= 4096) {
            size_t size = 4096 - offset_;
            (void)memcpy_s(&chunk_[offset_], size, src, size);
            len -= size;
            src += size;
            offset_ = 4096;
            flush();
        }
        if (len) {
            (void)memcpy_s(&chunk_[offset_], 4096 - offset_, src, len);
            offset_ += len;
        }
        pos_ += ori_len;
    }

    size_t getPos()
    {
        return pos_;
    }
};
#endif