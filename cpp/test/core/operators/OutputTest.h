#ifndef FLINK_TNEL_OUTPUTTEST_H
#define FLINK_TNEL_OUTPUTTEST_H

#include "core/io/RecordWriterOutput.h"
#include "core/operators/Output.h"
#include "core/streamrecord/StreamRecord.h"
#include "table/Row.h"
#include "table/data/GenericRowData.h"
#include "table/vectorbatch/VectorBatch.h"
// Instancing an Output view to retrieve records
class OutputTest : public Output {
private:
    StreamRecord *record_;
    Watermark *watermark_;
    std::vector<RowData *> outputs;

public:
    OutputTest() {};
    StreamRecord *getRecord() { return record_; };
    void collect(void *record) override {
        record_ = reinterpret_cast<StreamRecord *>(record);
        outputs.push_back(static_cast<RowData *>(record_->getValue()));
    };
    GenericRowData *getRow() {
        return (GenericRowData *)(record_->getValue());
    };
    std::vector<RowData *> &getAll() { return outputs; };
    void close() override {};
    void emitWatermark(Watermark *mark) override {
        watermark_ = mark;
        LOG("output emit watermark " << watermark_->getTimestamp());
    };
    Watermark *getWatermark() { return watermark_; };
    void emitWatermarkStatus(WatermarkStatus *status) override {};
};

// This is a dummy, will not write!!
class BatchOutputTest : public omnistream::datastream::RecordWriterOutput {
private:
    StreamRecord *record_;
    Watermark *watermark_;

public:
    BatchOutputTest() : RecordWriterOutput(nullptr, nullptr) {};
    auto getVectorBatch() const {
        return reinterpret_cast<omnistream::VectorBatch *>(record_->getValue());
    }
    auto getStreamRecord() { return record_; }
    void collect(void *record) override {
        record_ = reinterpret_cast<StreamRecord *>(record);
    };
    void emitWatermark(Watermark *mark) override { watermark_ = mark; }
    Watermark *getWatermark() { return watermark_; };
    void close() override {};
    void emitWatermarkStatus(WatermarkStatus *status) override {};
};

class OutputTestVectorBatch : public Output {
private:
    StreamRecord *record_;
    Watermark *watermark_;
    std::vector<omnistream::VectorBatch *> outputs;

public:
    OutputTestVectorBatch() {};
    StreamRecord *getRecord() { return record_; };
    void collect(void *record) override {
        outputs.push_back(static_cast<omnistream::VectorBatch *>(
            reinterpret_cast<StreamRecord *>(record)->getValue()));
    };
    GenericRowData *getRow() {
        return (GenericRowData *)(record_->getValue());
    };
    std::vector<omnistream::VectorBatch *> &getAll() { return outputs; };
    void close() override {};
    void emitWatermark(Watermark *mark) override { watermark_ = mark; };
    Watermark *getWatermark() { return watermark_; };
    void emitWatermarkStatus(WatermarkStatus *status) override {};
};

class TestCollector : public Collector {
public:
    void collect(void *record) {
        collectedData = reinterpret_cast<omnistream::VectorBatch *>(record);
    };
    void close() {};
    omnistream::VectorBatch *collectedData;
};

class ExternalOutputTest : public omnistream::datastream::RecordWriterOutput {
private:
    StreamRecord *record_;
    Watermark *watermark_;
    std::vector<Row *> outputs;

public:
    ExternalOutputTest() : RecordWriterOutput(nullptr, nullptr) {};
    StreamRecord *getRecord() { return record_; };
    void collect(void *record) override {
        outputs.push_back(static_cast<Row *>(
            reinterpret_cast<StreamRecord *>(record)->getValue()));
    };
    Row *getRow() { return (Row *)(record_->getValue()); };
    std::vector<Row *> &getAll() { return outputs; };
    void close() override {};
    void emitWatermark(Watermark *mark) override { watermark_ = mark; };
    Watermark *getWatermark() { return watermark_; };
    void emitWatermarkStatus(WatermarkStatus *status) override {};
};

class DeletingOutput : public omnistream::datastream::RecordWriterOutput {
public:
    DeletingOutput() : RecordWriterOutput(nullptr, nullptr) {};

    void collect(void *record) override {
        auto record_ = reinterpret_cast<StreamRecord *>(record);
        auto *vb =
            reinterpret_cast<omnistream::VectorBatch *>(record_->getValue());
        delete vb;
        delete record_;
    };
    void emitWatermark(Watermark *mark) override { delete mark; }
    void close() override {};
    void emitWatermarkStatus(WatermarkStatus *status) override {};
};
#endif