/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "DataStreamChainingOutput.h"
#include "basictypes/Object.h"

namespace omnistream::datastream {
    DataStreamChainingOutput::DataStreamChainingOutput(Input *op) : operator_(op) {
        // counter_ = new SimpleCounter();
        watermarkGauge = new WatermarkGauge();
    }

    void DataStreamChainingOutput::collect(void *record) {
        LOG(" ChainingOutput collect >>>>>>>>")
        auto streamRecord = reinterpret_cast<StreamRecord *>(record);
        auto *value = reinterpret_cast<Object *>(streamRecord->getValue());
        operator_->processElement(streamRecord);
        if (__builtin_expect(shouldFree, true)) {
            value->putRefCount();
        }
    }

    void DataStreamChainingOutput::close() {
        // do nothing
    }

    void DataStreamChainingOutput::emitWatermark(Watermark *mark) {
        LOG("ChainingOutput::emitWatermark: " << mark->getTimestamp() << " name: " << operator_->getName())
        watermarkGauge->setCurrentwatermark(mark->getTimestamp());
        // operator_->processWatermark(mark);
    }

    void DataStreamChainingOutput::emitWatermarkStatus(WatermarkStatus *watermarkStatus) {
        NOT_IMPL_EXCEPTION
    }

    void DataStreamChainingOutput::disableFree() {
        shouldFree = false;
    }
}