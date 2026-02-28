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

#ifndef OMNISTREAM_OMNISTREAMTASKNETWORKOUTPUT_H
#define OMNISTREAM_OMNISTREAMTASKNETWORKOUTPUT_H

#include <streaming/api/operators/OneInputStreamOperator.h>

#include "streaming/runtime/io/OmniPushingAsyncDataInput.h"
#include "streaming/api/operators/Input.h"
#include "runtime/metrics/Counter.h"
#include "runtime/metrics/SimpleCounter.h"

namespace omnistream {
    class OmniStreamTaskNetworkOutput : public OmniPushingAsyncDataInput::OmniDataOutput {
    public:
        explicit OmniStreamTaskNetworkOutput(OneInputStreamOperator* operator_,
                                             std::shared_ptr<omnistream::SimpleCounter> &numRecordsIn);

        void emitRecord(StreamRecord* streamRecord) override
        {
            LOG(">>>> OmniStreamTaskNetworkOutput.emitRecord")
            if (numRecordsIn != nullptr) {
                // here the value of streamRecord is not vectorBatch always
                // numRecordsIn->Inc(reinterpret_cast<omnistream::VectorBatch*>(streamRecord->getValue())->GetRowCount());
            }
            // later add condition

            if (taskType == 1) {
                operator_->processBatch(streamRecord);
            } else if (taskType == 2) {
                // auto currentOperator = dynamic_cast<OneInputStreamOperator *>(operator_);
                if (operator_->isSetKeyContextElement()) {
                    operator_->setKeyContextElement(streamRecord);
                }
                operator_->processElement(streamRecord);
            }
        };

        void emitWatermark(Watermark* watermark) override
        {
            LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermark")
            operator_->ProcessWatermark(watermark);
        }

        void emitWatermarkStatus(WatermarkStatus* watermarkStatus) override
        {
            LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermarkStatus")
            operator_->processWatermarkStatus(watermarkStatus);
        }

        void setTaskType(int taskType_)
        {
            this->taskType = taskType_;
        }

        ~OmniStreamTaskNetworkOutput() override = default;
    private:
        OneInputStreamOperator* operator_;
        std::shared_ptr<omnistream::SimpleCounter> numRecordsIn;
        int taskType;
    };
}
#endif
