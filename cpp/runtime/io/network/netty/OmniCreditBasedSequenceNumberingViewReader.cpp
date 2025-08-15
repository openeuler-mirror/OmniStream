/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "OmniCreditBasedSequenceNumberingViewReader.h"

namespace omnistream {
    OmniCreditBasedSequenceNumberingViewReader::
    OmniCreditBasedSequenceNumberingViewReader(ResultPartitionIDPOD partitionId,
                                               int subPartitionIndex,
                                               long outputBufferStatus)
        : outputBufferStatus(
            reinterpret_cast<OutputBufferStatus*>(outputBufferStatus))
    {
        LOG_TRACE("create OmniCreditBasedSequenceNumberingViewReader "
            << reinterpret_cast<long>(this))
        nettyBufferPool = new NettyBufferPool(bufferPoolSize, bufferSize);
    }

    OmniCreditBasedSequenceNumberingViewReader::~OmniCreditBasedSequenceNumberingViewReader()
    {
    }

    void OmniCreditBasedSequenceNumberingViewReader::notifyDataAvailable()
    {
        LOG_TRACE("notifyDataAvailable is invoked queue size by outputflusher")
        this->getNextBufferInternal();
    }

    void OmniCreditBasedSequenceNumberingViewReader::requestSubpartitionView(
        std::shared_ptr<ResultPartitionManager> resultPartitionManager,
        ResultPartitionIDPOD partitionId, int subPartitionId)
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        this->subpartitionView = resultPartitionManager->createSubpartitionView(
            partitionId, subPartitionId,
            BufferAvailabilityListener::shared_from_this());
        if (!this->subpartitionView) {
            LOG_TRACE("subpartitionView is null.........................");
            throw std::runtime_error("Subpartition view is null");
        }
    }

    int OmniCreditBasedSequenceNumberingViewReader::getAvailabilityAndBacklog()
    {
        // std::lock_guard<std::mutex> lock(queueMutex);
        // todo should invoke this to check if data is available
        auto queueSize = 0;
        {
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            queueSize = this->serializedBatchQueue.size();
        }
        LOG_TRACE(
            "OmniCreditBasedSequenceNumberingViewReaderN getAvailabilityAndBacklog "
            "queue size :"
            << queueSize)
        return queueSize;
    }

    void OmniCreditBasedSequenceNumberingViewReader::getNextBufferInternal()
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
        while (bufferAndLog) {
            std::shared_ptr<VectorBatchBuffer> vectorBatchBuffer = bufferAndLog->getBuffer();
            if (vectorBatchBuffer->GetSize() > 0) {
                // serialize data
                SerializeBufferAndBacklog(vectorBatchBuffer);
            } else {
                LOG("buffer size is 0, so i need to return " << std::this_thread::get_id())
                break;
            }
            // recycle buffer
            vectorBatchBuffer->RecycleBuffer();
            bufferAndLog = this->subpartitionView->getNextBuffer();
        }
    }

    int OmniCreditBasedSequenceNumberingViewReader::getNextBuffer()
    {
        int readElementNumber = 0;
        if (this->serializedBatchQueue.size() > 0) {
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            int dataSize = this->serializedBatchQueue.size();
            readElementNumber = dataSize > 10 ? 10 : dataSize;
            intptr_t dataResultContainer = this->outputBufferStatus->outputBuffer_;
            int position = 0;
            for (int i = 0; i < readElementNumber; i++) {
                std::shared_ptr<SerializedBatchInfo> serializedBatchInfo =
                    this->serializedBatchQueue.front();
                this->serializedBatchQueue.pop();
                long bufferAddress = 0;
                int bufferLength = 0;
                if (serializedBatchInfo->event != -1) {
                    bufferAddress = -1;
                    bufferLength = serializedBatchInfo->event;
                    INFO_RELEASE(">>>OmniCreditBasedSequenceNumberingViewReader pop an event from queue type"
                                 <<serializedBatchInfo->event << "from subpartitionView for "
                                 << reinterpret_cast<long>(this))
                } else {
                    bufferAddress =
                        reinterpret_cast<long>(serializedBatchInfo->buffer);
                    bufferLength = serializedBatchInfo->size;
                }
                LOG("bufferAddress: " << bufferAddress
                    << " bufferLength: " << bufferLength)
                *reinterpret_cast<uint64_t*>(dataResultContainer + position) =
                    bufferAddress;
                position += 8;
                *reinterpret_cast<uint32_t*>(dataResultContainer + position) =
                    bufferLength;
                position += 4;
            }
        }
        this->outputBufferStatus->numberElement = readElementNumber;
        return readElementNumber;
    }

    void OmniCreditBasedSequenceNumberingViewReader::DoSerializeVectorBatch(VectorBatch* element, int vectorSize,
                                                                            std::shared_ptr<NettyBufferInfo>&
                                                                            bufferInfo)
    {
        VectorBatchSerializationUtils::serializeVectorBatch(
            element, vectorSize, bufferInfo->GetAddress());
        bufferInfo->SetWrittenBytes(vectorSize);
        bufferInfo->IncrementElementNum();
    }

    bool OmniCreditBasedSequenceNumberingViewReader::SerializeVectorBatch(VectorBatch* element,
                                                                          std::shared_ptr<NettyBufferInfo>& bufferInfo)
    {
        int vectorSize = VectorBatchSerializationUtils::calculateVectorBatchSerializableSize(element);
        if (vectorSize > bufferSize - bufferInfo->elementNumBytes) {
            // send regular buffer to queue first
            AddNettyBufferInfoToQueue(bufferInfo);
            // allocate a new buffer
            auto bigBufferInfo = RequestNettyBuffer(vectorSize);
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
                return false;
            }
        }
    }

    bool OmniCreditBasedSequenceNumberingViewReader::DoSerializeWaterMark(long timestamp,
                                                                          std::shared_ptr<NettyBufferInfo> bufferInfo)
    {
        LOG("START TO SERIALIZE WATERMARK <<< " << timestamp)
        int dataSize = sizeof(int8_t) + sizeof(long);
        if (dataSize > bufferSize - bufferInfo->elementNumBytes) {
            // send regular buffer to queue first
            AddNettyBufferInfoToQueue(bufferInfo);
            // allocate a new buffer
            auto bigBufferInfo = RequestNettyBuffer(dataSize);
            VectorBatchSerializationUtils::SerializWatermark(
                timestamp, dataSize, bufferInfo->GetAddress());
            bigBufferInfo->SetWrittenBytes(dataSize);
            bigBufferInfo->IncrementElementNum();
            AddNettyBufferInfoToQueue(bigBufferInfo);
            return true;
        } else {
            if (bufferInfo->Useable(dataSize)) {
                VectorBatchSerializationUtils::SerializWatermark(
                    timestamp, dataSize, bufferInfo->GetAddress());
                bufferInfo->SetWrittenBytes(dataSize);
                bufferInfo->IncrementElementNum();
                return true;
            } else {
                // send data in buffer to queue
                AddNettyBufferInfoToQueue(bufferInfo);
                return false;
            }
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::AddNettyBufferInfoToQueue(
        std::shared_ptr<NettyBufferInfo>& bufferInfo)
    {
        if (bufferInfo->GetWrittenBytes() > 0) {
            VectorBatchSerializationUtils::SerializElementNum(bufferInfo->GetElementNum(),
                                                              bufferInfo->GetOriginalAddress());
            bufferInfo->MarkElementNumWritten();
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            SerializedBatchInfo serializedBatchInfo = {bufferInfo->GetOriginalAddress(), bufferInfo->GetWrittenBytes()};
            auto serializedBatchInfoPtr = std::make_shared<SerializedBatchInfo>(serializedBatchInfo);
            serializedBatchQueue.push(serializedBatchInfoPtr);
            bufferInfo = nullptr;
        }
    }

    std::shared_ptr<NettyBufferInfo> OmniCreditBasedSequenceNumberingViewReader::RequestNettyBuffer(int size)
    {
        int count = 0;
        std::shared_ptr<NettyBufferInfo> bufferInfo = nullptr;
        do {
            if (size > bufferSize) {
                bufferInfo = nettyBufferPool->RequestBigBuffer(size);
            } else {
                bufferInfo = nettyBufferPool->RequestBuffer();
            }
            if (!bufferInfo) {
                count++;
                if (count % noBufferPrintCount == 0) {
                    LOG("NO BUFFER AVAILABLE, waiting for 100 ms serializedBatchQueue:: " <<serializedBatchQueue.size())
                    INFO_RELEASE("NO BUFFER AVAILABLE, waiting for "
                        << requestNextBufferWaitingTime*noBufferPrintCount <<
                        " ms serializedBatchQueue:: "
                        <<serializedBatchQueue.size() << "  pointer = "
                        << reinterpret_cast<long>(this))
                    count = 0;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(requestNextBufferWaitingTime));
            }
        } while (!bufferInfo);
        return bufferInfo;
    }

    void OmniCreditBasedSequenceNumberingViewReader::RecycleNettyBuffer(long address)
    {
        nettyBufferPool->RecycleBuffer(address);
    }

    void OmniCreditBasedSequenceNumberingViewReader::SerializeBufferAndBacklog(
        std::shared_ptr<VectorBatchBuffer> vectorBatchBuffer)
    {
        if (vectorBatchBuffer->isBuffer()) {
            SerializeVectorBatchBuffer(vectorBatchBuffer);
        } else {
            SerializeEvent(vectorBatchBuffer);
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::SerializeEvent(
        std::shared_ptr<VectorBatchBuffer> vectorBatchBuffer)
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
                     << "from subpartitionView for " << reinterpret_cast<long>(this))
    }

    void OmniCreditBasedSequenceNumberingViewReader::SerializeVectorBatchBuffer(
        std::shared_ptr<VectorBatchBuffer> vectorBatchBuffer)
    {
        std::shared_ptr<ObjectSegment> objectSegment =
            vectorBatchBuffer->GetObjectSegment();
        int vectorBatchSize = vectorBatchBuffer->GetSize();
        auto offset = vectorBatchBuffer->GetOffset();
        auto bufferInfo = RequestNettyBuffer(bufferSize);
        for (int i = offset; i < vectorBatchSize + offset; i++) {
            StreamElement* streamElement = objectSegment->getObject(i);
            if (dynamic_cast<StreamRecord*>(streamElement)) {
                StreamRecord* streamRecord =
                    static_cast<StreamRecord*>(streamElement);
                // Handle StreamRecord
                // process streamRecord
                VectorBatch* element = static_cast<VectorBatch*>(
                    streamRecord->getValue());
                while (!SerializeVectorBatch(element, bufferInfo)) {
                    // it means buffer is not enough
                    bufferInfo = RequestNettyBuffer(bufferSize);
                }
                if (!bufferInfo) {
                    bufferInfo = RequestNettyBuffer(bufferSize);
                }
                omniruntime::vec::VectorHelper::FreeVecBatch(element);
                delete streamRecord;
            } else if (dynamic_cast<Watermark*>(streamElement)) {
                Watermark* watermark =
                    static_cast<Watermark*>(streamElement);
                // Handle Watermark
                long timestamp = watermark->getTimestamp();
                while (!DoSerializeWaterMark(timestamp, bufferInfo)) {
                    // it means buffer is not enough
                    bufferInfo = RequestNettyBuffer(bufferSize);
                }
            } else {
                LOG("Unknown stream element type");
                throw std::runtime_error(
                    "Unsupported stream element type");
            }
        }
        if (bufferInfo) {
            AddNettyBufferInfoToQueue(bufferInfo);
        }

        if (bufferInfo) {
            RecycleNettyBuffer(reinterpret_cast<long>(
                bufferInfo->GetOriginalAddress()));
        }
    }

    void OmniCreditBasedSequenceNumberingViewReader::DestroyNettyBufferPool()
    {
        INFO_RELEASE(
            "------- destroyNettyBufferPool, delete nettyBufferPool = ")
        if (nettyBufferPool) {
            delete nettyBufferPool;
            nettyBufferPool = nullptr;
        }
    }
} // namespace omnistream
