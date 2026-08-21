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
#include "OmniCreditBasedSequenceNumberingViewReader.h"
#include "runtime/buffer/ReadOnlySlicedNetworkBuffer.h"

namespace omnistream {
    OmniCreditBasedSequenceNumberingViewReader::
    OmniCreditBasedSequenceNumberingViewReader(ResultPartitionIDPOD partitionId,
                                               int subPartitionIndex,
                                               long outputBufferStatus,
                                               std::shared_ptr<LocalNettyBufferPool> localNettyBufferPool)
        : outputBufferStatus(
            reinterpret_cast<OutputBufferStatus *>(outputBufferStatus)),
          localNettyBufferPool_(localNettyBufferPool)
    {
        LOG_TRACE("create OmniCreditBasedSequenceNumberingViewReader "
            << reinterpret_cast<long>(this));
        bufferSize = localNettyBufferPool->getNettyBufferSize();
    }

    OmniCreditBasedSequenceNumberingViewReader::~OmniCreditBasedSequenceNumberingViewReader() {
        stopped_.store(true, std::memory_order_release);
        if (subpartitionView) {
            subpartitionView->releaseAllResources();
            subpartitionView.reset();
        }

        DestroyNettyBufferPool();

        if (networkBufferPendingRecycling.empty()) {
            return;
        }
        GErrorLog("When OmniCreditBasedSequenceNumberingViewReader is destroyed, "
            "there are still " + std::to_string(networkBufferPendingRecycling.size()) + " network buffers not recycled");
        for (auto it = networkBufferPendingRecycling.begin(); it != networkBufferPendingRecycling.end();) {
            it->second->RecycleBuffer();
            delete it->second; // this is ReadOnlySlicedNetworkBuffer, so we directly delete it
            it = networkBufferPendingRecycling.erase(it);
        }
    };

void OmniCreditBasedSequenceNumberingViewReader::notifyDataAvailable()
{
    LOG_TRACE("notifyDataAvailable is invoked queue size by outputflusher");
    this->getNextBufferInternal();
}

void OmniCreditBasedSequenceNumberingViewReader::requestSubpartitionView(
    std::shared_ptr<ResultPartitionManager> resultPartitionManager,
    ResultPartitionIDPOD partitionId,
    int subPartitionId)
{
    std::lock_guard<std::recursive_mutex> lock(queueMutex);
    this->subpartitionView = resultPartitionManager->createSubpartitionView(partitionId, subPartitionId, this);
    if (!this->subpartitionView) {
        LOG_TRACE("subpartitionView is null.........................");
        throw std::runtime_error("Subpartition view is null");
    }
}

int OmniCreditBasedSequenceNumberingViewReader::getAvailabilityAndBacklog()
{
    auto queueSize = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        queueSize = static_cast<int>(this->serializedBatchQueue.size());
    }
    LOG_TRACE(
        "OmniCreditBasedSequenceNumberingViewReaderN getAvailabilityAndBacklog "
        "queue size :"
        << queueSize);
    return queueSize;
}

void OmniCreditBasedSequenceNumberingViewReader::getNextBufferInternal()
{
    if (stopped_.load(std::memory_order_acquire)) {
        return;
    }
    if (!this->subpartitionView) {
        THROW_LOGIC_EXCEPTION("must be a bug , at this phase subpartitionView should not be null");
    }

        std::lock_guard<std::recursive_mutex> lock(fetchingDataMutex);
        BufferAndBacklog* bufferAndLog = this->subpartitionView->getNextBuffer();
        while (bufferAndLog) {
            Buffer* buffer = bufferAndLog->getBuffer();
            if (auto vectorBatchBuffer = dynamic_cast<VectorBatchBuffer*>(buffer)) {
                if (vectorBatchBuffer->GetSize() > 0) {
                    // serialize data
                    SerializeBufferAndBacklog(vectorBatchBuffer);
                } else {
                    LOG("buffer size is 0, so i need to return " << std::this_thread::get_id());
                    break;
                }
                // recycle buffer
                vectorBatchBuffer->RecycleBuffer();
                delete vectorBatchBuffer;
            } else if (auto nBuffer = dynamic_cast<datastream::ReadOnlySlicedNetworkBuffer*>(buffer)) {
                uint8_t *memorySegmentAddress = nBuffer->getMemorySegment()->getAll();
                int memorySegmentOffset = nBuffer->GetMemorySegmentOffset();
                uint8_t *readableAddress = memorySegmentAddress + memorySegmentOffset;
                int datasSize = nBuffer->GetSize();
                int bufferType = nBuffer->isBuffer() ? 1 : 2;
                SerializedBatchInfo serializedBatchInfo = {
                    readableAddress,readableAddress, datasSize,
                     bufferType
                };
                std::lock_guard<std::recursive_mutex> lock(queueMutex);
                auto serializedBatchInfoPtr =
                        std::make_shared<SerializedBatchInfo>(serializedBatchInfo);
                std::lock_guard<std::recursive_mutex> maplock(recycleNetworkBufferMutex);
                networkBufferPendingRecycling.insert({reinterpret_cast<int64_t>(readableAddress), nBuffer});
                serializedBatchQueue.push(serializedBatchInfoPtr);
            } else {
                THROW_RUNTIME_ERROR("Unknown buffer type in getNextBufferInternal");
            }

        delete bufferAndLog;
        bufferAndLog = this->subpartitionView->getNextBuffer();
    }
}

    int OmniCreditBasedSequenceNumberingViewReader::getNextBuffer()
    {
        int readElementNumber = 0;
        if (this->serializedBatchQueue.size() > 0) {
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            size_t dataSize = this->serializedBatchQueue.size();
            readElementNumber = dataSize > 10 ? 10 : dataSize;
            uintptr_t dataResultContainer = this->outputBufferStatus->outputBuffer_;
            unsigned int position = 0;
            for (int i = 0; i < readElementNumber; i++) {
                std::shared_ptr<SerializedBatchInfo> serializedBatchInfo =
                        this->serializedBatchQueue.front();
                this->serializedBatchQueue.pop();

                long memorySegmentAddress = reinterpret_cast<long>(serializedBatchInfo->memorySegmentAddress);
                long dataAddress = reinterpret_cast<long>(serializedBatchInfo->dataAddress);
                int bufferLength = serializedBatchInfo->dataSize;
                int bufferType = serializedBatchInfo->bufferType;

                LOG("bufferAddress: " << dataAddress << " bufferLength: " << bufferLength);
                * reinterpret_cast<uint64_t *>(dataResultContainer + position) = memorySegmentAddress;
                position += 8;
                * reinterpret_cast<uint64_t *>(dataResultContainer + position) = dataAddress;
                position += 8;
                *reinterpret_cast<uint32_t *>(dataResultContainer + position) = bufferLength;
                position += 4;
                *reinterpret_cast<uint32_t *>(dataResultContainer + position) = bufferType;
                position += 4;
            }
        }

    this->outputBufferStatus->numberElement = static_cast<int32_t>(readElementNumber);
    return readElementNumber;
}

    void OmniCreditBasedSequenceNumberingViewReader::DoSerializeVectorBatch(VectorBatch *element, int vectorSize,
                                                                            std::shared_ptr<NettyBufferInfo> &
                                                                            bufferInfo)
    {
        if (!bufferInfo) {
            INFO_RELEASE("buffer info in DoSerializeVectorBatch is null");
            throw std::runtime_error("buffer info in DoSerializeVectorBatch is null");
        }
        VectorBatchSerializationUtils::serializeVectorBatch(
            element, vectorSize, bufferInfo->GetPosition());
        bufferInfo->SetWrittenBytes(vectorSize);
        bufferInfo->IncrementElementNum();
    }

    bool OmniCreditBasedSequenceNumberingViewReader::SerializeVectorBatch(VectorBatch *element,
                                                                          std::shared_ptr<NettyBufferInfo> &bufferInfo)
    {
        int vectorSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(element);
        if (!bufferInfo) {
            INFO_RELEASE("buffer info in SerializeVectorBatch is null");
            throw std::runtime_error("buffer info in SerializeVectorBatch is null");
        }
        if (vectorSize > bufferSize - bufferInfo->elementNumBytes) {
            // send regular buffer to queue first
            AddNettyBufferInfoToQueue(bufferInfo);
            // allocate a new big buffer
            auto bigBufferNettyMemeorySegment = RequestNettyBuffer(vectorSize);
            bigBufferNettyMemeorySegment->EnableEligibleRecycling();
            auto bigBufferInfo = std::make_shared<NettyBufferInfo>(bigBufferNettyMemeorySegment);

            DoSerializeVectorBatch(element, vectorSize, bigBufferInfo);
            AddNettyBufferInfoToQueue(bigBufferInfo);
            return true;
        } else {
            if (bufferInfo->Useable(vectorSize)) {
                DoSerializeVectorBatch(element, vectorSize, bufferInfo);
                return true;
            } else {
                // send data in buffer to queue
                AddNettyBufferInfoToQueue(bufferInfo);
                currentInUseNettyMemorySegment->EnableEligibleRecycling();
                if (currentInUseNettyMemorySegment->GetRefCount() == 0)
                {
                    //recycle it
                    RecycleNettyBuffer(reinterpret_cast<long>(currentInUseNettyMemorySegment->GetOriginalAddress()));
                }
                currentInUseNettyMemorySegment = nullptr;
                return false;
            }
        }
    }

    bool OmniCreditBasedSequenceNumberingViewReader::DoSerializeWaterMark(long timestamp,
                                                                          std::shared_ptr<NettyBufferInfo> bufferInfo)
    {
        LOG("START TO SERIALIZE WATERMARK <<< " << timestamp);
        int dataSize = sizeof(int8_t) + sizeof(long);
        if (bufferInfo->Useable(dataSize)) {
            VectorBatchSerializationUtils::SerializWatermark(
                timestamp, dataSize, bufferInfo->GetPosition());
            bufferInfo->SetWrittenBytes(dataSize);
            bufferInfo->IncrementElementNum();
            return true;
        } else {
            // send data in buffer to queue
            AddNettyBufferInfoToQueue(bufferInfo);
            currentInUseNettyMemorySegment->EnableEligibleRecycling();
            if (currentInUseNettyMemorySegment->GetRefCount() == 0)
            {
                //recycle it
                RecycleNettyBuffer(reinterpret_cast<long>(currentInUseNettyMemorySegment->GetOriginalAddress()));
            }
            currentInUseNettyMemorySegment = nullptr;
            return false;
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::AddNettyBufferInfoToQueue(
        std::shared_ptr<NettyBufferInfo> &bufferInfo)
    {
        if (bufferInfo->GetWrittenBytes() > 0) {
            VectorBatchSerializationUtils::SerializElementNum(bufferInfo->GetElementNum(),
                                                              bufferInfo->GetDataAddress());
            bufferInfo->MarkElementNumWritten();
            // bufferInfo->IncrementElementNum();//todo ? why increase here

            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            SerializedBatchInfo serializedBatchInfo = {bufferInfo->GetOriginalAddress(), bufferInfo->GetDataAddress(), bufferInfo->GetWrittenBytes()};
            auto serializedBatchInfoPtr = std::make_shared<SerializedBatchInfo>(serializedBatchInfo);
            serializedBatchQueue.push(serializedBatchInfoPtr);
            bufferInfo = nullptr;
        }
    }

    std::shared_ptr<NettyMemorySegment> OmniCreditBasedSequenceNumberingViewReader::RequestNettyBuffer(int size)
    {
        if (localNettyBufferPool_) {
            // Use new two-tier pool with condition_variable-based blocking
            if (size + NettyBufferInfo::elementNumBytes > bufferSize) {
                return localNettyBufferPool_->requestBigBuffer(size);
            }
            return localNettyBufferPool_->requestBufferBlocking();
        }else
        {
            throw std::runtime_error("localNettyBufferPool_ is null");
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::RecycleNettyBuffer(long address)
    {
        if (localNettyBufferPool_) {
            localNettyBufferPool_->recycleBuffer(address);
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::SerializeBufferAndBacklog(
            VectorBatchBuffer* vectorBatchBuffer)
    {
        if (vectorBatchBuffer->isBuffer()) {
            SerializeVectorBatchBuffer(vectorBatchBuffer);
        } else {
            //this is not no longer supported, because all the event are serialized in memorySegment format
            // SerializeEvent(vectorBatchBuffer);
            throw std::runtime_error("vectorBatchBuffer should not be in event type..........");
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::SerializeEvent(
            VectorBatchBuffer* vectorBatchBuffer)
    {
        int evenType = vectorBatchBuffer->EventType();
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        SerializedBatchInfo serializedBatchInfo = {
            nullptr, 0,
            evenType
        };
        auto serializedBatchInfoPtr =
                std::make_shared<SerializedBatchInfo>(serializedBatchInfo);
        serializedBatchQueue.push(serializedBatchInfoPtr);
        INFO_RELEASE(">>>OmniCreditBasedSequenceNumberingViewReader push an event to queue type: "<< evenType
            << "from subpartitionView for " << reinterpret_cast<long>(this));
    }

    void OmniCreditBasedSequenceNumberingViewReader::SerializeVectorBatchBuffer(
            VectorBatchBuffer* vectorBatchBuffer)
    {
        ObjectSegment *objectSegment = vectorBatchBuffer->GetObjectSegment();
        int vectorBatchSize = vectorBatchBuffer->GetSize();
        auto offset = vectorBatchBuffer->GetOffset();

        // std::shared_ptr<NettyBufferInfo> bufferInfo = nullptr;

        if (!currentInUseNettyMemorySegment)
        {
            currentInUseNettyMemorySegment = RequestNettyBuffer(bufferSize);
        }
        std::shared_ptr<NettyBufferInfo> bufferInfo = std::make_shared<NettyBufferInfo>(currentInUseNettyMemorySegment);

        for (int i = offset; i < vectorBatchSize + offset; i++) {
            StreamElement *streamElement = objectSegment->getObject(i);
            if (dynamic_cast<StreamRecord *>(streamElement)) {
                StreamRecord *streamRecord =
                        static_cast<StreamRecord *>(streamElement);
                // Handle StreamRecord
                // process streamRecord
                VectorBatch *element = static_cast<VectorBatch *>(
                    streamRecord->getValue());
                while (!SerializeVectorBatch(element, bufferInfo)) {
                    bufferInfo = CreateNettyBufferInfo();
                }

                delete element;
                delete streamRecord;
            } else if (dynamic_cast<Watermark *>(streamElement)) {
                Watermark *watermark =
                        static_cast<Watermark *>(streamElement);
                // Handle Watermark
                long timestamp = watermark->getTimestamp();
                while (!DoSerializeWaterMark(timestamp, bufferInfo)) {
                    bufferInfo = CreateNettyBufferInfo();
                }
            } else {
                THROW_RUNTIME_ERROR("Unsupported stream element type");
            }

            if (!bufferInfo)
            {
                bufferInfo = std::make_shared<NettyBufferInfo>(currentInUseNettyMemorySegment);
            }
        }
        if (bufferInfo)
        {
            AddNettyBufferInfoToQueue(bufferInfo);
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::DestroyNettyBufferPool()
    {
        INFO_RELEASE(
            "------- destroyNettyBufferPool, delete nettyBufferPool = ");
        if (localNettyBufferPool_) {
            if (currentInUseNettyMemorySegment)
            {
                currentInUseNettyMemorySegment->EnableEligibleRecycling();
                RecycleNettyBuffer(reinterpret_cast<long>(currentInUseNettyMemorySegment->GetOriginalAddress()));
            }
            localNettyBufferPool_->lazyDestroy();
            localNettyBufferPool_.reset();
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::RecycleNetworkBuffer(long address)
    {
        std::lock_guard<std::recursive_mutex> lock(recycleNetworkBufferMutex);
        auto it = networkBufferPendingRecycling.find(address);
        if (it != networkBufferPendingRecycling.end()) {
            it->second->RecycleBuffer();
            delete it->second; // this is ReadOnlySlicedNetworkBuffer, so we directly delete it
            networkBufferPendingRecycling.erase(it);
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::ResumeConsumption()
    {
        this->subpartitionView->resumeConsumption();
    }

    std::shared_ptr<NettyBufferInfo> OmniCreditBasedSequenceNumberingViewReader::CreateNettyBufferInfo()
    {
        {
            auto nettyMemorySegment = RequestNettyBuffer(bufferSize);
            currentInUseNettyMemorySegment = nettyMemorySegment;
            auto bufferInfo = std::make_shared<NettyBufferInfo>(currentInUseNettyMemorySegment);
            return bufferInfo;
        }
    }
} // namespace omnistream
