/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef COPYINGBROADCASTINGOUTPUTCOLLECTOR_H
#define COPYINGBROADCASTINGOUTPUTCOLLECTOR_H

#include <streamrecord/StreamRecordHelper.h>
#include <task/WatermarkGaugeExposingOutput.h>
#include "table/Row.h"

namespace omnistream {
    class VectorBatchCopyingBroadcastingOutputCollector : public WatermarkGaugeExposingOutput {

    public:
        explicit VectorBatchCopyingBroadcastingOutputCollector(std::vector<WatermarkGaugeExposingOutput*> outputs) : outputs(std::move(outputs)) {};

        void collect(void * record) override
        {
            LOG(" VectorBatchCopyingBroadcastingOutputCollector collect >>>>>>>>")
            for (size_t i = 0; i < outputs.size() - 1; i++) {
                auto output = outputs[i];
                auto streamRecord = reinterpret_cast<StreamRecord*>(record);
                if (streamRecord->hasExternalRow()) {
                    long timestamp = streamRecord->getTimestamp();
                    auto value = streamRecord->getValue();
                    auto row = reinterpret_cast<Row*>(value);
                    auto copyRow = new Row(row->getKind(), row->getFieldByPosition(),
                        row->getFieldByName(), row->getPositionByName());
                    auto copyStreamRecord = new StreamRecord(copyRow, timestamp);
                    output->collect(copyStreamRecord);
                    delete row;
                    delete streamRecord;
                } else {
                    output->collect(StreamRecordHelper::deepCopyVectorBatch(streamRecord));
                }
            }
            if (!outputs.empty()) {
                outputs[outputs.size() - 1]->collect(record);
            }
        }

        void close() override {}

        void emitWatermark(Watermark *watermark) override
        {
            for (auto output : outputs) {
                output->emitWatermark(watermark);
            }
        }

        void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override
        {
            for (auto output : outputs) {
                output->emitWatermarkStatus(watermarkStatus);
            }
        }

    private:
        std::vector<WatermarkGaugeExposingOutput*> outputs;
    };
}

namespace omnistream::datastream {
    class CopyingBroadcastingOutputCollector : public WatermarkGaugeExposingOutput {

    public:
        explicit CopyingBroadcastingOutputCollector(std::vector<WatermarkGaugeExposingOutput*> outputs) : outputs(std::move(outputs)) {};

        void collect(void * record) override
        {
            LOG(" CopyingBroadcastingOutputCollector collect >>>>>>>>")
            if (outputs.empty()) {
                auto streamRecord = reinterpret_cast<StreamRecord*>(record);
                Object *value = reinterpret_cast<Object *>(streamRecord->getValue());
                value->putRefCount();
                return;
            }

            for (size_t i = 0; i < outputs.size() - 1; i++) {
                auto streamRecord = reinterpret_cast<StreamRecord*>(record);
                outputs[i]->collect(StreamRecordHelper::deepCopyObject(streamRecord));
            }

            if (!outputs.empty()) {
                outputs[outputs.size() - 1]->collect(record);
            }
        }

        void close() override {}

        void emitWatermark(Watermark *watermark) override
        {
            for (auto output : outputs) {
                output->emitWatermark(watermark);
            }
        }

        void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override
        {
            for (auto output : outputs) {
                output->emitWatermarkStatus(watermarkStatus);
            }
        }

    private:
        std::vector<WatermarkGaugeExposingOutput*> outputs;
    };
}
#endif //COPYINGBROADCASTINGOUTPUTCOLLECTOR_H
