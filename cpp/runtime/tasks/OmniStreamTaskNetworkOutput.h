/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/3/25.
//

#ifndef OMNISTREAM_OMNISTREAMTASKNETWORKOUTPUT_H
#define OMNISTREAM_OMNISTREAMTASKNETWORKOUTPUT_H

#include "io/OmniPushingAsyncDataInput.h"
#include "operators/Input.h"
#include "runtime/metrics/Counter.h"
#include "runtime/metrics/SimpleCounter.h"

namespace omnistream {
    class OmniStreamTaskNetworkOutput : public OmniPushingAsyncDataInput::OmniDataOutput {
    public:
        explicit OmniStreamTaskNetworkOutput(Input* operator_,
                                             std::shared_ptr<omnistream::SimpleCounter> &numRecordsIn);

        void emitRecord(StreamRecord* streamRecord) override {
            LOG(">>>> OmniStreamTaskNetworkOutput.emitRecord")
            if (numRecordsIn != nullptr) {
                numRecordsIn->Inc(reinterpret_cast<omnistream::VectorBatch*>(streamRecord->getValue())->GetRowCount());
            }
            operator_->processBatch(streamRecord);
        };

        void emitWatermark(Watermark* watermark) override {
            LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermark")
            operator_->ProcessWatermark(watermark);
        }

        void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override {
            LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermarkStatus")
            operator_->processWatermarkStatus(watermarkStatus);
        }

        ~OmniStreamTaskNetworkOutput() override = default;
    private:
        Input* operator_;
        std::shared_ptr<omnistream::SimpleCounter> numRecordsIn;
    };
}
#endif //OMNISTREAM_OMNISTREAMTASKNETWORKOUTPUT_H
