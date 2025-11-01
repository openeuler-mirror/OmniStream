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

#include "OmniLocalChannelReader.h"

namespace omnistream {
    OmniLocalChannelReader::OmniLocalChannelReader(ResultPartitionIDPOD partitionId, int subPartitionIndex,
                                                   long outputBufferStatus, std::string taskNameWithSubtask)
        : partitionId_(partitionId), subPartitionIndex_(subPartitionIndex),
          memorySegmentInfo(reinterpret_cast<MemorySegmentInfo *>(outputBufferStatus)),
          taskNameWithSubtask_(std::move(taskNameWithSubtask))
    {
    }

    void OmniLocalChannelReader::requestSubpartitionView(
        std::shared_ptr<ResultPartitionManager> resultPartitionManager,
        ResultPartitionIDPOD partitionId, int subPartitionId)
    {
        std::lock_guard<std::recursive_mutex> lock(createViewMutex);
        this->subpartitionView = resultPartitionManager->createSubpartitionView(
            partitionId, subPartitionId,
            BufferAvailabilityListener::shared_from_this());
        if (!this->subpartitionView) {
            INFO_RELEASE("local reader subpartitionView is null.........................");
            throw std::runtime_error("Subpartition view is null");
        }
    }

    void OmniLocalChannelReader::notifyDataAvailable()
    {
        std::unique_lock<std::recursive_mutex> lock(dataAvailableMutex);
        dataAvailable = true;
        dataAvailableCondition.notify_one();
    }

    bool OmniLocalChannelReader::checkIfDataAvailable()
    {
        std::unique_lock<std::recursive_mutex> lock(dataAvailableMutex);
        if (isStopped) {
            INFO_RELEASE("OmniLocalChannelReader is stopped, no data available.");
            return false;
        }
        dataAvailableCondition.wait(lock, [this] {
            bool wait = !dataAvailable && !isStopped;
            if (wait) {
                INFO_RELEASE(
                    "************* JNI INVOCATION FOR " << taskNameWithSubtask_ <<
                    " checkIfDataAvailable in OmniLocalChannelReader IS WAITING")
            }
            return !wait;
        });
        dataAvailable = false;
        return true;
    }

    uint8_t OmniLocalChannelReader::getNextBuffer()
    {
        if (!this->subpartitionView) {
            LOG("must be a bug , at this phase subpartitionView should not be "
                "null");
            throw std::runtime_error(
                "Subpartition view is null--------------------------");
        }

        std::lock_guard<std::recursive_mutex> lock(fetchingDataMutex);
        std::shared_ptr<BufferAndBacklog> bufferAndLog =
                this->subpartitionView->getNextBuffer();

        if (bufferAndLog) {
            std::shared_ptr<Buffer> buffer = bufferAndLog->getBuffer();
            if (buffer->GetSize() > 0) {
                if (auto vBuffer = std::dynamic_pointer_cast<VectorBatchBuffer>(buffer)) {
                    // handle VectorBatchBuffer specific processing, maybe will implement it in the future
                    INFO_RELEASE(
                        "VectorBatchBuffer is not supported in OmniLocalChannelReader, so it will be recycled directly.......");
                    buffer->RecycleBuffer();
                    throw std::logic_error("VectorBatchBuffer is not supported in OmniLocalChannelReader");
                } else if (auto nBuffer = std::dynamic_pointer_cast<datastream::ReadOnlySlicedNetworkBuffer>(buffer)) {
                    WrapBufferInfoIntoMemorySegmentInfo(
                        nBuffer, bufferAndLog);
                    return 1;
                } else {
                    INFO_RELEASE("Unknown buffer type!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    buffer->RecycleBuffer();
                    throw std::logic_error("Unknown buffer type in OmniLocalChannelReader");
                }
            } else {
                LOG("buffer size is 0, so i need to return " << std::this_thread::get_id());
                buffer->RecycleBuffer();
                return 0;
            }
        } else {
            return 0;
        }
    }

    void OmniLocalChannelReader::WrapBufferInfoIntoMemorySegmentInfo(
        std::shared_ptr<::datastream::ReadOnlySlicedNetworkBuffer> nBuffer,
        std::shared_ptr<BufferAndBacklog> bufferAndLog)
    {
        uint8_t *memorySegmentAddress = nBuffer->getMemorySegment()->getAll();
        int offset = nBuffer->GetMemorySegmentOffset();
        int numBytes = nBuffer->GetSize();
        int sequenceNumber = bufferAndLog->getSequenceNumber();
        int backlog = bufferAndLog->getBuffersInBacklog();
        int nextDataType = 0;
        if (bufferAndLog->getNextDataType() == ObjectBufferDataType::DATA_BUFFER) {
            nextDataType = 1;
        } else if (bufferAndLog->getNextDataType() == ObjectBufferDataType::EVENT_BUFFER) {
            nextDataType = 2;
        }
        int currentDataType = nBuffer->isBuffer() ? 1 : 2;
        memorySegmentInfo->memorySegmentAddress = reinterpret_cast<uint64_t>(memorySegmentAddress);
        memorySegmentInfo->readIndex = offset;
        memorySegmentInfo->length = numBytes;
        memorySegmentInfo->currentDataType = currentDataType;
        memorySegmentInfo->backlog = backlog;
        memorySegmentInfo->nextDataType = nextDataType;
        memorySegmentInfo->sequenceNumber = sequenceNumber;
        pendingRecyclingBuffer = nBuffer;
    }

    void OmniLocalChannelReader::recycleMemorySegment(long memorySegmentAddress)
    {
        if (pendingRecyclingBuffer) {
            pendingRecyclingBuffer->RecycleBuffer();
            pendingRecyclingBuffer = nullptr;
        }
    }

    void OmniLocalChannelReader::Close()
    {
        std::unique_lock<std::recursive_mutex> lock(dataAvailableMutex);
        isStopped = true;
        dataAvailableCondition.notify_one();
    }

    void OmniLocalChannelReader::ReleaseAllResources()
    {
        Close();
        recycleMemorySegment(0);
        if (subpartitionView) {
            subpartitionView->releaseAllResources();
            subpartitionView = nullptr;
        }
    }

    void OmniLocalChannelReader::ResumeConsumption()
    {
        subpartitionView->resumeConsumption();
    }
}
