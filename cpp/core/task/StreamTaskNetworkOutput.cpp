/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "StreamTaskNetworkOutput.h"
#include "../metrics/SimpleCounter.h"
#include "../operators/OneInputStreamOperator.h"
#include "../operators/AbstractTwoInputStreamOperator.h"


// StreamTaskNetworkOutput::StreamTaskNetworkOutput(StreamOperator *op) : operator_(op)
// {
// }

namespace omnistream::datastream {
    void StreamTaskNetworkOutput::emitRecord(StreamRecord *record)
    {
        LOG("emitRecord >>>>>>>>")
        counter_->inc();

        LOG("after counter_ operator address " + std::to_string(reinterpret_cast<long>(streamOperator)))
        // LOG("after counter_ operator name " + std::string(operator_->getName()))
        LOG("stream record address  " + std::to_string(reinterpret_cast<long>(record)))
        // funcPtr(streamOperator, record);
        recordProcessfuncPtr(record);
    }

    StreamTaskNetworkOutput::StreamTaskNetworkOutput(StreamOperator *streamOperator, int operatorMethodIndicator)
    {
        if (operatorMethodIndicator == 0) {
            OneInputStreamOperator* oneInputStreamOperator = static_cast<OneInputStreamOperator*>(streamOperator);
            if (oneInputStreamOperator->isSetKeyContextElement()) {
                recordProcessfuncPtr = [oneInputStreamOperator](StreamRecord *record) {
                    LOG("operatorMethodIndicator=0, setKey");
                    oneInputStreamOperator->setKeyContextElement(record);
                    LOG("operatorMethodIndicator=0, processElement");
                    oneInputStreamOperator->processElement(record);
                };
            } else {
                recordProcessfuncPtr = [oneInputStreamOperator](StreamRecord *record) {
                    LOG("operatorMethodIndicator=0, processElement");
                    oneInputStreamOperator->processElement(record);
                };
            }
        } else if (operatorMethodIndicator == 1) {
            AbstractTwoInputStreamOperator *twoInputStreamOperator = static_cast<AbstractTwoInputStreamOperator *>(streamOperator);

            if (twoInputStreamOperator->isSetKeyContextElement1()) {
                recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord *record) {
                    twoInputStreamOperator->setKeyContextElement1(record);
                    twoInputStreamOperator->processElement1(record);
                };
            } else {
                recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord *record) { twoInputStreamOperator->processElement1(record); };
            }
        } else if (operatorMethodIndicator == 2) {
            AbstractTwoInputStreamOperator *twoInputStreamOperator = static_cast<AbstractTwoInputStreamOperator *>(streamOperator);
            if (twoInputStreamOperator->isSetKeyContextElement2()) {
                recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord *record) {
                    twoInputStreamOperator->setKeyContextElement2(record);
                    twoInputStreamOperator->processElement2(record);
                };
            } else {
                recordProcessfuncPtr = [twoInputStreamOperator](StreamRecord *record) { twoInputStreamOperator->processElement2(record); };
            }
        }
        counter_ = new SimpleCounter();
    }
}
