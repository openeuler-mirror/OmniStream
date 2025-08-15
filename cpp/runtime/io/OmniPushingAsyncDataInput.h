//
// Created by root on 3/1/25.
//

#ifndef OMNISTREAM_OMNIPUSHINGASYNCDATAINPUT_H
#define OMNISTREAM_OMNIPUSHINGASYNCDATAINPUT_H


#include "AvailabilityProvider.h"
#include <io/DataInputStatus.h>
#include "io/DataOutput.h"
#include "functions/Watermark.h"
#include "watermark/WatermarkStatus.h"
#include <streamrecord/StreamRecord.h>

namespace omnistream {

    class OmniPushingAsyncDataInput : public AvailabilityProvider{
    public:
        class OmniDataOutput {
        public:
            virtual void emitRecord(StreamRecord* streamRecord) = 0;

            virtual void emitWatermark(Watermark* watermark) = 0; // 

            virtual void emitWatermarkStatus(WatermarkStatus* watermarkStatus) = 0;

            virtual void close() {};

            virtual ~OmniDataOutput() = default;
        };

        virtual DataInputStatus emitNext(OmniDataOutput* output) = 0;
    };

} // omnistream

#endif //OMNISTREAM_OMNIPUSHINGASYNCDATAINPUT_H
