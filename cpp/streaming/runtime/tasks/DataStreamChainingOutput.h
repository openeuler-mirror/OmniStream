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

#ifndef OMNISTREAM_DATASTREAMCHAININGOUTPUT_H
#define OMNISTREAM_DATASTREAMCHAININGOUTPUT_H

#include "WatermarkGaugeExposingOutput.h"
#include "streaming/runtime/streamrecord/StreamElement.h"
#include "../../api/operators/Input.h"
#include "../metrics/WatermarkGauge.h"
#include "runtime/watermark/WatermarkStatus.h"
namespace omnistream::datastream {
    class DataStreamChainingOutput : public WatermarkGaugeExposingOutput {
    public:
        explicit DataStreamChainingOutput(Input* op);
        void collect(void *record) override;
        void close() override;
        void emitWatermark(Watermark* mark) override;
        void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override;
        void disableFree();

        ~DataStreamChainingOutput() override;

    private:
        Input *operator_;
        WatermarkGauge *watermarkGauge;
        bool shouldFree{true};
    };
}

#endif
