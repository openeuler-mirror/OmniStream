/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "RemoteInputChannel.h"
#include "table/utils/VectorBatchDeserializationUtils.h"
#include "common.h"

namespace omnistream
{

    RemoteInputChannel::RemoteInputChannel(std::shared_ptr<SingleInputGate> inputGate, int channelIndex, ResultPartitionIDPOD partitionId,
                                           std::shared_ptr<ResultPartitionManager> partitionManager,
                                           int initialBackoff, int maxBackoff, int networkBuffersPerChannel, std::shared_ptr<Counter> numBytesIn,
                                           std::shared_ptr<Counter> numBuffersIn) : LocalInputChannel(inputGate, channelIndex, partitionId, partitionManager, initialBackoff, maxBackoff, numBytesIn, numBuffersIn), initialCredit(networkBuffersPerChannel)
    {
    }

    void RemoteInputChannel::requestSubpartition(int subpartitionIndex)
    {
        // remote version, no need to implement
    }

    void RemoteInputChannel::notifyRemoteDataAvailable(long bufferAddress, int bufferLength, int sequenceNumber)
    {
        if (bufferAddress == -1)
        {
            // event
            int eventType = bufferLength;
            LOG("remote got an event data:::: event type: " << eventType)
            INFO_RELEASE("remote got an event data:::: event type: " << eventType)
            auto eventData =  std::make_shared<VectorBatchBuffer>(eventType);
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            this->dataQueue.push(eventData);
        } else {
            uint8_t* buffer = reinterpret_cast<uint8_t*>(bufferAddress);
            // do data deserialization
            std::shared_ptr<ObjectSegment> objectSegment = this->DoDataDeserializationResult(buffer, bufferLength);
            auto vectorBatchBuffer = std::make_shared<VectorBatchBuffer>(objectSegment);
            vectorBatchBuffer->SetSize(objectSegment->getSize());
            std::lock_guard<std::recursive_mutex> lock(queueMutex);
            this->dataQueue.push(vectorBatchBuffer);
            LOG("remote got an buffer  "<< vectorBatchBuffer->ToDebugString(true));
        }
        this->notifyDataAvailable();
    }

    std::optional<BufferAndAvailability> RemoteInputChannel::getNextBuffer()
    {
        std::lock_guard<std::recursive_mutex> lock(queueMutex);
        if (this->dataQueue.size() == 0)
        {
            return std::nullopt;
        }

        auto vectorBatchBuffer = this->dataQueue.front();
        this->dataQueue.pop();

        ObjectBufferDataType dataType = ObjectBufferDataType::NONE;

        int backlogSize = this->dataQueue.size();
        if (backlogSize > 0)
        {
            dataType = ObjectBufferDataType::DATA_BUFFER;
        }

        std::shared_ptr<ObjectBuffer> data = std::shared_ptr<ObjectBuffer>(vectorBatchBuffer);

        return BufferAndAvailability{data, dataType, backlogSize, expectSequenceNumber++};
    }

    void RemoteInputChannel::getResData(void* dst, void* src, size_t cur, int res)
    {
        svbool_t pg = svwhilelt_b8(0, res);
        svuint8_t s = svld1(pg, reinterpret_cast<uint8_t*>(src) + cur);
        svst1(pg, reinterpret_cast<uint8_t*>(dst) + cur, s);
    }

    std::shared_ptr<ObjectSegment> RemoteInputChannel::DoDataDeserializationResult(uint8_t*& buffer, int bufferLength)
    {
        LOG("----DoDataDeserializationResult start 1:: " << buffer << " bufferLength:: " <<
            bufferLength)
        int32_t elementNum;

        size_t len = sizeof(int32_t);
        size_t skip_num = 32;
        size_t num = len / skip_num;
        svbool_t pTrue = svptrue_b8();
        size_t cur = 0;
        for (size_t i = 0; i < num; i++)
        {
            svuint8_t s = svld1(pTrue, reinterpret_cast<uint8_t*>(buffer) + cur);
            svst1(pTrue, reinterpret_cast<uint8_t*>(&elementNum) + cur, s);
            cur += skip_num;
        }
        getResData(&elementNum, buffer, cur, len - cur);
        buffer += sizeof(int32_t);
        std::shared_ptr<ObjectSegment> objectSegment = std::make_shared<ObjectSegment>(elementNum);
        LOG("----DoDataDeserializationResult start 2:: " << buffer << " bufferLength:: " <<
            bufferLength)
        for (int32_t i = 0; i < elementNum; i++) {
            int8_t dataType;
            memcpy(&dataType, buffer, sizeof(int8_t));
            buffer += sizeof(int8_t);
            LOG("----DoDataDeserializationResult start 3:: " << (buffer) << " bufferLength:: " <<
                bufferLength)
            StreamElementTag tagType = static_cast<StreamElementTag>(dataType);
            switch (tagType) {
                case StreamElementTag::TAG_WATERMARK:
                    {
                        long timestamp = VectorBatchDeserializationUtils::derializeWatermark(buffer);
                        LOG("RemoteInputChannel::DoDataDeserializationResult:: deserialize watermark :: "<< timestamp)
                        Watermark* watermark = new Watermark(timestamp);
                        objectSegment->putObject(i, watermark);
                        break;
                    }
                case StreamElementTag::VECTOR_BATCH:
                    {
                        VectorBatch* vb = VectorBatchDeserializationUtils::deserializeVectorBatch(buffer);
                        StreamRecord* streamRecord = new StreamRecord(vb);
                        objectSegment->putObject(i, streamRecord);
                        break;
                    }
                default:
                    break;
            }
        }
        return objectSegment;
        // no need to implement
    }

} // namespace omnistream