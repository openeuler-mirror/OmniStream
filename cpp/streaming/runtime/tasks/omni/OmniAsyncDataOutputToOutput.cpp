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
#include "OmniAsyncDataOutputToOutput.h"

namespace omnistream {
    OmniAsyncDataOutputToOutput::OmniAsyncDataOutputToOutput(Output* output_, bool isDataStream)
        : output(output_), isDataStream(isDataStream) {
        reuse = new StreamRecord();
    }

    OmniAsyncDataOutputToOutput::~OmniAsyncDataOutputToOutput()
    {
        delete reuse;
    }

    void OmniAsyncDataOutputToOutput::emitRecord(StreamRecord *streamRecord)
    {
        LOG(">>>> OmniAsyncDataOutputToOutput.emitRecord")
        if (isDataStream) {
            output->collect(streamRecord);
        } else {
            reuse->replace(streamRecord->getValue(), streamRecord->getTimestamp());
            output->collect(reuse);
        }
    }

    void OmniAsyncDataOutputToOutput::emitWatermark(Watermark *watermark)
    {
        LOG(">>>> OmniAsyncDataOutputToOutput.emitWatermark")
        output->emitWatermark(watermark);
    }

    void OmniAsyncDataOutputToOutput::emitWatermarkStatus(WatermarkStatus *watermarkStatus)
    {
        LOG(">>>> OmniAsyncDataOutputToOutput.emitWatermarkStatus")
        output->emitWatermarkStatus(watermarkStatus);
    }
}