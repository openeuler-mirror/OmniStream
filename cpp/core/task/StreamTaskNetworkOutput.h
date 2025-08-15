/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 8/14/24.
//

#ifndef FLINK_TNEL_STREAMTASKNETWORKOUTPUT_H
#define FLINK_TNEL_STREAMTASKNETWORKOUTPUT_H

#include <functional>
#include "../io/DataOutput.h"
#include "../operators/Input.h"
#include "../metrics/Counter.h"
#include "functions/StreamElement.h"
#include "../operators/StreamOperator.h"

namespace omnistream::datastream {
    class StreamTaskNetworkOutput : public DataOutput {

    public:
        explicit StreamTaskNetworkOutput();
        void emitRecord(StreamRecord *record) override;

        StreamTaskNetworkOutput(class StreamOperator* streamOperator, int operatorMethodIndicator);

        std::function<void(StreamRecord *)> recordProcessfuncPtr;

    private:
        // Input *operator_;

        StreamOperator *streamOperator;

        Counter *counter_;
    };
}


#endif //FLINK_TNEL_STREAMTASKNETWORKOUTPUT_H
