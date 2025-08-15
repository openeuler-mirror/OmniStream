/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2025. All rights reserved.
 */

#include "SinkWriterOperator.h"

SinkWriterOperator::SinkWriterOperator(KafkaSink* kafkaSink,
                                       const nlohmann::json& config)
    : kafkaSink(kafkaSink),
    endOfInput(false),
    description(config),
    isDataStream(!config["batch"]) {
    if (config["batch"]) {
        inputTypes = config["inputTypes"].get<std::vector<std::basic_string<char>>>();
    }
    initializeState();
    isDataStream = !description["batch"];
}

void SinkWriterOperator::initializeState()
{
    this->sinkWriter = kafkaSink->CreateWriter();
}

void SinkWriterOperator::open() {
}

RowData* SinkWriterOperator::getOutputEntireRow(omnistream::VectorBatch *vecBatch, int rowId)
{
    int colsCount = vecBatch->GetVectorCount();
    BinaryRowData *row = BinaryRowData::createBinaryRowDataWithMem(colsCount);
    for (int32_t i = 0; i < colsCount; i++) {
        int pos = i;
        if (inputTypes[i] == "BIGINT" || inputTypes[i].find("TIMESTAMP") != std::string::npos) {
            row->setLong(i, reinterpret_cast<omniruntime::vec::Vector<int64_t> *>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "INTEGER") {
            row->setInt(i, reinterpret_cast<omniruntime::vec::Vector<int32_t> *>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "DOUBLE") {
            row->setLong(i, reinterpret_cast<omniruntime::vec::Vector<double> *>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "BOOLEAN") {
            row->setInt(i, reinterpret_cast<omniruntime::vec::Vector<bool> *>(vecBatch->Get(pos))->GetValue(rowId));
        } else if (inputTypes[i] == "STRING" || inputTypes[i] == "VARCHAR" || inputTypes[i] == "VARCHAR(2147483647)") {
            auto str =
                    reinterpret_cast
                    <omniruntime::vec::Vector<omniruntime::vec::LargeStringContainer<std::string_view>> *>
                    (vecBatch->Get(pos))->GetValue(rowId);
            auto strVal = std::make_unique<std::u32string>(str.begin(), str.end());
            row->setString(i, new BinaryStringData(strVal.get()));
        } else {
            LOG("Data type not supported: " << inputTypes[i])
            throw std::runtime_error("Data type not supported");
        };
    }
    row->setRowKind(vecBatch->getRowKind(rowId));
    return row;
}

void SinkWriterOperator::processBatch(StreamRecord *record)
{
    if (record->hasExternalRow()) {
        Row *row_input = reinterpret_cast<Row *>(record->getValue());
        sinkWriter->write(row_input);
    } else {
        // vectorbatch convert to rowdata
        omnistream::VectorBatch *input = reinterpret_cast<omnistream::VectorBatch *>(record->getValue());
        int rowCount = input->GetRowCount();
        for (int row = 0; row < rowCount; ++row) {
            RowData *currentRow = getOutputEntireRow(input, row);
            sinkWriter->write(currentRow);
        }
    }
}

void SinkWriterOperator::processElement(StreamRecord *record)
{
    String* input = reinterpret_cast<String*>(record->getValue());
    sinkWriter->write(input);
}

void SinkWriterOperator::EndInput()
{
    endOfInput = true;
    sinkWriter->Flush(true);

    emitCommittables<KafkaCommittable>(std::numeric_limits<std::int64_t>::max());
}

template <typename CommT>
void SinkWriterOperator::emitCommittables(std::int64_t checkpointId)
{
    std::vector<CommT> committables = sinkWriter->prepareCommit();
    int indexOfThisSubtask = 0;
    int numberOfParallelSubtasks = 1;

    emit(indexOfThisSubtask, numberOfParallelSubtasks, checkpointId, committables);
}

template <typename CommT>
void SinkWriterOperator::emit(int indexOfThisSubtask,
                              int numberOfParallelSubtasks,
                              std::int64_t checkpointId,
                              const std::vector<CommT>& committables) {}

void SinkWriterOperator::ProcessWatermark(Watermark *watermark) {}

void SinkWriterOperator::processWatermarkStatus(WatermarkStatus *watermarkStatus) {}

void SinkWriterOperator::initializeState(StreamTaskStateInitializerImpl *initializer, TypeSerializer *keySerializer)
{
    subtaskIndex = initializer->getEnvironment()->getTaskInfo()->getIndexOfThisSubtask();
    this->sinkWriter->SetSubTaskIdx(subtaskIndex);
}


template void SinkWriterOperator::emitCommittables<KafkaCommittable>(std::int64_t checkpointId);
template void SinkWriterOperator::emit(int indexOfThisSubtask,
                                       int numberOfParallelSubtasks,
                                       std::int64_t checkpointId,
                                       const std::vector<KafkaCommittable>& committables);
