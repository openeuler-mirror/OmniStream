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

#ifndef OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
#define OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H

#include <utility>
#include <runtime/io/network/api/serialization/RecordDeserializer.h>
#include <runtime/io/network/api/serialization/SpillingAdaptiveSpanningRecordDeserializer.h>
#include <runtime/plugable/DeserializationDelegate.h>
#include <runtime/plugable/NonReusingDeserializationDelegate.h>
#include <streaming/runtime/streamrecord/StreamElementSerializer.h>

#include "OmniOperatorJIT/core/src/vector/vector_helper.h"
#include "OmniStreamTaskInput.h"
#include "buffer/NetworkBuffer.h"
#include "event/EndOfData.h"
#include "event/EndOfPartitionEvent.h"
#include "runtime/io/network/api/serialization/EventSerializer.h"
#include "partition/consumer/InputChannelInfo.h"
#include "partition/consumer/InputGate.h"
#include "streaming/runtime/io/OmniStreamTaskNetworkOutput.h"
#include "typeutils/TypeSerializer.h"
#include "runtime/io/checkpointing/CheckpointedInputGate.h"
namespace omnistream {
class OmniAbstractStreamTaskNetworkInput : public OmniStreamTaskInput {
public:
    OmniAbstractStreamTaskNetworkInput(int64_t inputIndex, std::shared_ptr<CheckpointedInputGate> inputGate, int taskType,
        TypeSerializer *inputSerializer, std::vector<long> & channelInfos)
        : inputIndex(inputIndex), inputGate(std::move(inputGate)), taskType(taskType), currentRecordDeserializer(nullptr), output_(nullptr)
    {
        inSerializer = inputSerializer;
        deserializationDelegate_ = new NonReusingDeserializationDelegate(
                std::make_unique<datastream::StreamElementSerializer>(inputSerializer));
        recordDeserializers = getRecordDeserializers(channelInfos);
        rowCount = 0;
        maxRowCount = 1000;
        timeout = 1000;
        running_.exchange(true);
    }

    DataInputStatus emitNext(OmniPushingAsyncDataInput::OmniDataOutput *output) override
    {
        // we might need reconstruct here
        if (auto curOutput = dynamic_cast<OmniStreamTaskNetworkOutput*>(output)) {
            curOutput->setTaskType(taskType);
        }

        if (taskType == 1) {
            fromOriginal = inputGate->fromOriginal();
            if (fromOriginal) {
                if (!isStartTimer) {
                    INFO_RELEASE("Start timer thread")
                    timer_thread_ = std::thread(&OmniAbstractStreamTaskNetworkInput::timerThread, this);
                    isStartTimer = true;
                }
                output_ = output;
                return processForSQLFromOriginal(output);
            } else {
                return processForSQL(output);
            }
        } else if (taskType == 2) {
            return processForDataStream(output);
        } else {
            throw std::runtime_error("Unknown taskType: " + taskType);
        }
    }

    void timerThread() {
        while (running_) {
            std::unique_lock<std::mutex> lock(mutex_);
            if (cv_.wait_for(lock, std::chrono::seconds(1), [this]() {
                return !running_ || rowList.empty();
            })) {
                if (!running_) break;
            } else {
                if (!rowList.empty()) {
                    lock.unlock();
                    INFO_RELEASE("Raw to Native, triggering by schedule")
                    emitCurrentBatch(output_);
                }
            }
        }
        INFO_RELEASE("timer thread end")
    }


    std::shared_ptr<CompletableFuture> GetAvailableFuture() override
    {
        // no inputGate no output
        return AVAILABLE;
    }
    std::unique_ptr<std::unordered_map<long, datastream::RecordDeserializer *>> getRecordDeserializers(
    std::vector<long> & channelInfos)
    {
        std::unique_ptr<std::unordered_map<long, datastream::RecordDeserializer *>> recordDeserializers
                = std::make_unique<std::unordered_map<long, datastream::RecordDeserializer *>>();
        for (size_t i = 0; i < channelInfos.size(); i++) {
            LOG("getRecordDeserializers channelInfo " << i)
            auto deserializer = new datastream::SpillingAdaptiveSpanningRecordDeserializer();
            (*recordDeserializers)[channelInfos.at(i)] = deserializer;
        }
        return recordDeserializers;
    }

    [[nodiscard]] datastream::RecordDeserializer *getActiveSerializer(long channelInfo) const
    {
        return (*recordDeserializers)[channelInfo];
    }

    DataInputStatus processBufferOrEventOptForSQL(OmniPushingAsyncDataInput::OmniDataOutput *output,
                                                  std::optional<std::shared_ptr<BufferOrEvent>>& bufferOrEventOpt)
    {
        isLastValueNull = false;
        NullValueCount = 0;

        LOG(">>>>> bufferOrEventOpt has value")
        auto bufferOrEvent = bufferOrEventOpt.value();
        LOG(">>>>> bufferOrEventOpt bufferOrEvent" +
            std::to_string(reinterpret_cast<int64_t>(bufferOrEvent.get())))
        if (bufferOrEvent->isBuffer()) {
            auto buff = std::reinterpret_pointer_cast<ObjectBuffer>(bufferOrEvent->getBuffer());

            auto size = buff->GetSize();
            auto objSegment = buff->GetObjectSegment();
            auto offset = buff->GetOffset();
            LOG(">>>>object segment is " << std::to_string(reinterpret_cast<long>(objSegment.get())))
            LOG(">>>>>buffer size is " << size << " buffer offset is " << offset)

            LOG("===================start output=======================")
            for (int64_t index = offset; index < offset + size; index++) {
                StreamElement *object = objSegment->getObject(index);
                LOG("OmniAbstractStreamTaskNetworkInput tag: " << static_cast<int>(object->getTag()))
                if (object->getTag() == StreamElementTag::TAG_REC_WITH_TIMESTAMP ||
                    object->getTag() == StreamElementTag::TAG_REC_WITHOUT_TIMESTAMP) {
                    auto record = static_cast<StreamRecord *>(object);
                    auto vectorBatch = static_cast<VectorBatch *>(record->getValue());
                    size_t row_cnt = vectorBatch->GetRowCount();
                    numberOfRow += row_cnt;

                    output->emitRecord(reinterpret_cast<StreamRecord *>(object));
                } else if (object->getTag() == StreamElementTag::TAG_WATERMARK) {
                    output->emitWatermark(reinterpret_cast<Watermark *>(object));
                }
            }
            // more avaiable means there could be more data come in
            buff->RecycleBuffer();
            return DataInputStatus::MORE_AVAILABLE;
        } else {
            // we got event
            std::shared_ptr<AbstractEvent> event = bufferOrEvent->getEvent();
            // so far, we only knows
            DataInputStatus status = processEvent(event);
            return status;
        }
    }

    DataInputStatus processForSQL(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        while (true) {
            auto bufferOrEventOpt = inputGate->PollNext();
            if (bufferOrEventOpt) {
                return processBufferOrEventOptForSQL(output, bufferOrEventOpt);
            } else {
                if (isLastValueNull) {
                    NullValueCount++;
                }
                isLastValueNull = true;
                if (NullValueCount % 100 == 1) {
                    // LOG(">>>>> bufferOrEventOpt has NO value, found continousely " << NullValueCount)
                }
                return DataInputStatus::NOTHING_AVAILABLE;
            }
        }
    }

    void processBufferForDataStreamAndSQLFromOriginal(std::shared_ptr<BufferOrEvent> bufferOrEvent)
    {
        auto buffer = std::static_pointer_cast<ReadOnlySlicedNetworkBuffer>(bufferOrEvent->getBuffer());
        auto inputChannelInfo = bufferOrEvent->getChannelInfo();
        currentRecordDeserializer = getActiveSerializer(inputChannelInfo.getInputChannelIdx());
        if (currentRecordDeserializer == nullptr) {
            THROW_LOGIC_EXCEPTION("currentRecordDeserializer has already been released");
        }
        currentRecordDeserializer->SetNextBuffer(buffer);
    }

    DataInputStatus processFullRecordForDataStream(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        auto *element = static_cast<StreamElement *>(deserializationDelegate_->getInstance());
        if (element->getTag() == StreamElementTag::TAG_WATERMARK) {
            output->emitWatermark(reinterpret_cast<Watermark *>(element));
        } else {
            processElement(element, output);
        }
        return DataInputStatus::MORE_AVAILABLE;
    }

    DataInputStatus processForDataStream(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        while (true) {
            if (currentRecordDeserializer != nullptr) {
                DeserializationResult &result = currentRecordDeserializer->getNextRecord(*deserializationDelegate_);

                if (unlikely(result.isBufferConsumed())) {
                    LOG("isBufferConsumed: do we really buffer consumed?!!!")
                    currentRecordDeserializer = nullptr;
                }

                if (likely(result.isFullRecord())) {
                    return processFullRecordForDataStream(output);
                }
            }

            auto bufferOrEventOpt = inputGate->PollNext();
            if (bufferOrEventOpt) {
                auto bufferOrEvent = bufferOrEventOpt.value();
                if (bufferOrEvent->isBuffer()) {
                    processBufferForDataStreamAndSQLFromOriginal(bufferOrEvent);
                } else  {
                    std::cout << "current is event" << std::endl;
                    std::shared_ptr<AbstractEvent> event = bufferOrEvent->getEvent();
                    DataInputStatus status = processEvent(event);
                    return status;
                }
            } else {
                return DataInputStatus::NOTHING_AVAILABLE;
            }
        }
    }

    DataInputStatus processForSQLFromOriginal(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        while (true) {
            if (currentRecordDeserializer != nullptr) {
                DeserializationResult &result = currentRecordDeserializer->getNextRecord(*deserializationDelegate_);

                if (unlikely(result.isBufferConsumed())) {
                    LOG("isBufferConsumed: do we really buffer consumed?!!!")
                    currentRecordDeserializer = nullptr;
                }

                if (likely(result.isFullRecord())) {
                    return processFullRecordForSQLFromOriginal(output);
                }
            }

            auto bufferOrEventOpt = inputGate->PollNext();
            if (bufferOrEventOpt) {
                auto bufferOrEvent = bufferOrEventOpt.value();
                if (bufferOrEvent->isBuffer()) {
                    processBufferForDataStreamAndSQLFromOriginal(bufferOrEvent);
                } else  {
                    std::cout << "current is event" << std::endl;
                    std::shared_ptr<AbstractEvent> event = bufferOrEvent->getEvent();
                    DataInputStatus status = processEvent(event, output);
                    return status;
                }
            } else {
                return DataInputStatus::NOTHING_AVAILABLE;
            }
        }
    }

    DataInputStatus processFullRecordForSQLFromOriginal(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        auto *element = reinterpret_cast<StreamElement *>(deserializationDelegate_->getInstance()); // 这里面包装的是BinaryRowData
        if (element->getTag() == StreamElementTag::TAG_WATERMARK) {
            output->emitWatermark(reinterpret_cast<Watermark *>(element));
        } else {
            auto record = reinterpret_cast<StreamRecord *>(element);
            auto row = reinterpret_cast<BinaryRowData *>(record->getValue());

            // 1.If rowList is empty, reset the batch start time
            if (rowList.empty()) {
                batchStartTime = std::chrono::steady_clock::now();
            }

            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (rowList.empty()) {
                    batchStartTime = std::chrono::steady_clock::now();
                }

                // 2.Push the row data into list
                auto newRow = reinterpret_cast<BinaryRowData *>(row->copy());
                rowList.push_back(newRow);
                LOG("push in a record, size is " << rowList.size())
                rowCount++;
                numberOfRow++;
            }

            // 3.Calculate the elapsed time
            auto currentTime = std::chrono::steady_clock::now();
            auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - batchStartTime).count();
            // 4.If the number of records reaches the maximum capacity is reached
            if (rowCount >= maxRowCount || elapsedMs >= timeout) {
                LOG("Reach the maximum capacity or timeout, start to generate VectorBatch and send to output")
                // 3. Convert the rowList to VectorBatch, send to output, and clear the state
                emitCurrentBatch(output);
            }
        }
        return DataInputStatus::MORE_AVAILABLE;
    }

    void emitCurrentBatch(OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        if (rowCount == 0) {
            return;
        }
        // Convert the rowdata list to VectorBatch
        StreamRecord* batchRecord = nullptr;
        const std::vector<std::string>& inputTypes = reinterpret_cast<BinaryRowDataSerializer *>(inSerializer)->getInputTypes();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            omnistream::VectorBatch *resultBatch = createOutputBatch(rowList, inputTypes);
            batchRecord = new StreamRecord(resultBatch);
            for (BinaryRowData* row : rowList) {
                delete row;
            }
            rowList.clear();
            rowCount = 0;
            cv_.notify_one();
        }
        output->emitRecord(batchRecord);

        batchStartTime = std::chrono::steady_clock::now();
    }

    omnistream::VectorBatch* createOutputBatch(std::vector<BinaryRowData*> collectedRows,
                                               const std::vector<std::string>& inputTypes)
    {
        INFO_RELEASE("Start to createOutputBatch")
        int numColumns = inputTypes.size();
        auto inputRowType =  new std::vector<omniruntime::type::DataTypeId>;
        for (const auto &typeStr : inputTypes) {
            inputRowType->push_back(LogicalType::flinkTypeToOmniTypeId(typeStr));
        }
        int numRows = collectedRows.size();
        INFO_RELEASE("collectedRows: " << collectedRows.size())
        auto* outputBatch = new omnistream::VectorBatch(numRows);
        for (int colIndex = 0; colIndex < numColumns; ++colIndex) {
            switch (inputRowType->at(colIndex)) {
                case DataTypeId::OMNI_LONG:
                case DataTypeId::OMNI_TIMESTAMP_WITHOUT_TIME_ZONE:
                case DataTypeId::OMNI_TIMESTAMP: {
                    setLong(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_INT: {
                    setInt(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_DOUBLE: {
                    setDouble(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_BOOLEAN: {
                    setBool(outputBatch, numRows, colIndex, rowList);
                    break;
                }
                case DataTypeId::OMNI_CHAR:
                case DataTypeId::OMNI_VARCHAR: {
                    setString(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_DECIMAL64: {
                    setDecimal64(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                case DataTypeId::OMNI_DECIMAL128: {
                    setDecimal128(outputBatch, numRows, colIndex, collectedRows);
                    break;
                }
                default: {
                    throw std::runtime_error("Unsupported column type in inputRow");
                }
            }
        }

        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            outputBatch->setRowKind(rowIndex, collectedRows[rowIndex]->getRowKind());
        }
        return outputBatch;
    }

    void setInt(omniruntime::vec::VectorBatch* outputBatch,
                int numRows, int colIndex, std::vector<BinaryRowData*> collectedRows)
    {
        auto *vector = new omniruntime::vec::Vector<int32_t>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getInt(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setLong(omniruntime::vec::VectorBatch* outputBatch,
                 int numRows, int colIndex, std::vector<BinaryRowData*> collectedRows)
    {
        auto *vector = new omniruntime::vec::Vector<int64_t>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setDecimal64(omniruntime::vec::VectorBatch* outputBatch,
                      int numRows, int colIndex, std::vector<BinaryRowData*> collectedRows) {
        auto *vector = new omniruntime::vec::Vector<int64_t>(numRows, DataTypeId::OMNI_DECIMAL64);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setDecimal128(omniruntime::vec::VectorBatch* outputBatch,
                       int numRows, int colIndex, std::vector<BinaryRowData*> collectedRows) {
        auto *vector = new omniruntime::vec::Vector<Decimal128>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getDecimal128(colIndex, 0));
            }
        }
        outputBatch->Append(vector);
    }


    void setString(omniruntime::vec::VectorBatch* outputBatch,
                   int numRows, int colIndex, std::vector<BinaryRowData*> collectedRows)
    {
        using VarcharVector = omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>>;
        VarcharVector *vector = new VarcharVector(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                auto value = std::string(collectedRows[rowIndex]->getStringView(colIndex));
                omniruntime::vec::VectorHelper::VectorSetValue<DataTypeId::OMNI_VARCHAR>(vector, rowIndex, (void*)&value);
            }
        }
        outputBatch->Append(vector);
    }

    void setDouble(omniruntime::vec::VectorBatch* outputBatch,
                   int numRows, int colIndex, std::vector<BinaryRowData*> collectedRows)
    {
        auto *vector = new omniruntime::vec::Vector<double>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getLong(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    void setBool(omniruntime::vec::VectorBatch* outputBatch,
                 int numRows, int colIndex, std::vector<BinaryRowData*> collectedRows)
    {
        auto *vector = new omniruntime::vec::Vector<bool>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; ++rowIndex) {
            if (collectedRows[rowIndex]->isNullAt(colIndex)) {
                vector->SetNull(rowIndex);
            } else {
                vector->SetValue(rowIndex, *collectedRows[rowIndex]->getBool(colIndex));
            }
        }
        outputBatch->Append(vector);
    }

    int getInputIndex() override
    {
        return static_cast<int>(inputIndex);
    }

    void close() override
    {
        running_.exchange(false);
        INFO_RELEASE("OmniAbstractStreamTaskNetworkInput received numberOfRow: " << numberOfRow)
    }

protected:
    int64_t inputIndex;
    std::shared_ptr<CheckpointedInputGate> inputGate;
    std::atomic<long> numberOfRow {0};

    void processElement(StreamElement *recordOrMark, OmniDataOutput *output)
    {
        output->emitRecord(static_cast<StreamRecord *>(recordOrMark));
    }

    DataInputStatus processEvent(std::shared_ptr<AbstractEvent> event)
    {
        if (dynamic_cast<EndOfData *>(event.get())) { // END_OF_USER_RECORDS_EVENT is End_of_Data
            if (inputGate->HasReceivedEndOfData()) {
                return DataInputStatus::END_OF_DATA;
            }
        } else if (dynamic_cast<EndOfPartitionEvent *>(event.get())) {
            // it means one sub partition or channel end. we need to check if all end by checking input gate state
            if (inputGate->IsFinished()) {
                return DataInputStatus::END_OF_INPUT;
            }
        }
        // by default,continue the data processing
        return DataInputStatus::MORE_AVAILABLE;
    }

    // Specifically for SQL from original task
    DataInputStatus processEvent(std::shared_ptr<AbstractEvent> event, OmniPushingAsyncDataInput::OmniDataOutput *output)
    {
        if (dynamic_cast<EndOfData *>(event.get())) { // END_OF_USER_RECORDS_EVENT is End_of_Data
            if (inputGate->HasReceivedEndOfData()) {
                // Which means reach the end of Data, if the rowList still remains data, create the last vectorbatch and send to output
                emitCurrentBatch(output);
                return DataInputStatus::END_OF_DATA;
            }
        } else if (dynamic_cast<EndOfPartitionEvent *>(event.get())) {
            // it means one sub partition or channel end. we need to check if all end by checking input gate state
            if (inputGate->IsFinished()) {
                return DataInputStatus::END_OF_INPUT;
            }
        }
        // by default,continue the data processing
        return DataInputStatus::MORE_AVAILABLE;
    }

private:
    // for troubleshooting
    int NullValueCount = 0;
    bool isLastValueNull = false;
    int taskType;
    std::unique_ptr<std::unordered_map<long, datastream::RecordDeserializer *>> recordDeserializers;
    datastream::RecordDeserializer* currentRecordDeserializer;
    DeserializationDelegate* deserializationDelegate_;
    TypeSerializer *inSerializer;
    // Determine if the upstream task is an original Java task.
    bool fromOriginal = true;
    // When the current SQL task has a Java task as its upstream,
    // maintain the number of rows accumulated for generating VectorBatch.
    int rowCount;
    // Maximum number of rows supported by VectorBatch
    int maxRowCount;
    // Maximum timeout for accumulating VectorBatch(ms)
    int timeout;
    // List of BinaryRowData to be accumulated, only init in SQLFromOriginal case
    std::vector<BinaryRowData *> rowList;
    std::chrono::steady_clock::time_point batchStartTime;

    // timerThread相关
    std::atomic<bool> running_{false};
    std::mutex mutex_;           // 保护缓冲区的互斥锁
    std::condition_variable cv_; // 用于线程同步的条件变量
    std::thread timer_thread_;   // 定时器线程
    OmniPushingAsyncDataInput::OmniDataOutput *output_;
    bool isStartTimer = false;
};
}  // namespace omnistream

#endif  // OMNISTREAM_OMNIABSTRACTSTREAMTASKNETWORKINPUT_H
