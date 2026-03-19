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

#ifndef CHANNELSTATEPERSISTER_H
#define CHANNELSTATEPERSISTER_H
#include <memory>

#include "partition/consumer/InputChannelInfo.h"
#include "checkpoint/channel/ChannelStateWriter.h"
#include "io/network/api/serialization/EventSerializer.h"

namespace omnistream {

class ChannelStatePersister {
    enum class CheckpointStatus {
        COMPLETED,
        BARRIER_PENDING,
        BARRIER_RECEIVED
    };
public:
    explicit ChannelStatePersister(const std::shared_ptr<ChannelStateWriter> &channelStateWriter,
        const InputChannelInfo &channelInfo)
        : channelStateWriter_(channelStateWriter), channelInfo_(channelInfo)
    {
    }

    void StartPersisting(int64_t barrierId, const std::vector<Buffer*>& knownBuffers)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        LOG("StartPersisting" << barrierId)

        if (checkpointStatus_ == CheckpointStatus::BARRIER_RECEIVED && lastSeenBarrier_ > barrierId) {
            throw std::runtime_error(
                "Barrier for newer checkpoint " + std::to_string(lastSeenBarrier_) +
                " has already been received compared to the requested checkpoint " +
                std::to_string(barrierId)
            );
        }

        if (lastSeenBarrier_ < barrierId) {
            checkpointStatus_ = CheckpointStatus::BARRIER_PENDING;
            lastSeenBarrier_ = barrierId;
        }

        if (!knownBuffers.empty()) {
            channelStateWriter_->AddInputData(
                barrierId,
                channelInfo_,
                ChannelStateWriter::sequenceNumberUnknown,
                knownBuffers
            );
        }
    }

    void StopPersisting(int64_t id)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        LOG("StopPersisting" << id)
        if (id >= lastSeenBarrier_) {
            checkpointStatus_ = CheckpointStatus::COMPLETED;
            lastSeenBarrier_ = id;
        }
    }

    void MaybePersist(Buffer* buffer)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        if (checkpointStatus_ == CheckpointStatus::BARRIER_PENDING && buffer->isBuffer()) {
            std::vector<Buffer*> buffers;
            Buffer *inflightbuffer = buffer->RetainBuffer();
            if (inflightbuffer != nullptr) {
                buffers.push_back(inflightbuffer);
                channelStateWriter_->AddInputData(lastSeenBarrier_, channelInfo_,
                    ChannelStateWriter::sequenceNumberUnknown, buffers);
            } else {
                LOG_DEBUG(" buffers is null  ");
                buffer->RecycleBuffer();
            }
        }
    }

    std::optional<int64_t> CheckForBarrier(Buffer* buffer)
    {
        std::lock_guard<std::mutex> persistLock(mutex_);
        auto event = ParseEvent(buffer);

        if (event == nullptr) {
            return std::nullopt;
        }
        if (auto barrier = std::dynamic_pointer_cast<CheckpointBarrier>(event)) {
            int64_t barrierId = barrier->GetId();
            int64_t expectedBarrierId = (checkpointStatus_ == CheckpointStatus::COMPLETED)
                ? lastSeenBarrier_ + 1
                : lastSeenBarrier_;

            if (barrierId >= expectedBarrierId) {
                LOG("found barrier" << barrierId)
                checkpointStatus_ = CheckpointStatus::BARRIER_RECEIVED;
                lastSeenBarrier_ = barrierId;
                return std::make_optional(lastSeenBarrier_);
            } else {
                LOG("ignoring barrier" << barrierId)
            }
        }

        return std::nullopt;
    }

    std::shared_ptr<AbstractEvent> ParseEvent(Buffer* buffer)
    {
        if (buffer->isBuffer()) {
            return nullptr;
        } else {
            std::shared_ptr<AbstractEvent> event = EventSerializer::fromBuffer(buffer);
            // 重置buffer的读取位置
            buffer->SetReaderIndex(0);
            return event;
        }
    }

    bool HasBarrierReceived() const
    {
        return checkpointStatus_ == CheckpointStatus::BARRIER_RECEIVED;
    }

private:
    std::string CheckpointStatusToString() const
    {
        switch (checkpointStatus_) {
            case CheckpointStatus::COMPLETED: return "COMPLETED";
            case CheckpointStatus::BARRIER_PENDING: return "BARRIER_PENDING";
            case CheckpointStatus::BARRIER_RECEIVED: return "BARRIER_RECEIVED";
            default: return "UNKNOWN";
        }
    }
private:
    InputChannelInfo channelInfo_;
    CheckpointStatus checkpointStatus_ = CheckpointStatus::COMPLETED;
    long lastSeenBarrier_ = -1L;
    std::shared_ptr<ChannelStateWriter> channelStateWriter_;
    std::mutex mutex_;
};
}
#endif