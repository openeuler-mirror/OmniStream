/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 */

#ifndef OMNISTREAM_OMNIPUSHINGASYNCDATAINPUT_H
#define OMNISTREAM_OMNIPUSHINGASYNCDATAINPUT_H


#include "runtime/io/AvailabilityProvider.h"
#include <streaming/runtime/io/DataInputStatus.h>
#include "streaming/runtime/io/DataOutput.h"
#include "streaming/api/watermark/Watermark.h"
#include "watermark/WatermarkStatus.h"
#include <streaming/runtime/streamrecord/StreamRecord.h>

namespace omnistream {

    class OmniPushingAsyncDataInput : public AvailabilityProvider {
    public:
        class OmniDataOutput {
        public:
            virtual void emitRecord(StreamRecord* streamRecord) = 0;

            virtual void emitWatermark(Watermark* watermark) = 0;

            virtual void emitWatermarkStatus(WatermarkStatus* watermarkStatus) = 0;

            virtual void close() {};

            virtual ~OmniDataOutput() = default;
        };

        virtual DataInputStatus emitNext(OmniDataOutput* output) = 0;
    };

} // omnistream

#endif
