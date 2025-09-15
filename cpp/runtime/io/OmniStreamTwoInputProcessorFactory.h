/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#ifndef OMNISTREAM_OMNISTREAMTWOINPUTPROCESSORFACTORY_H
#define OMNISTREAM_OMNISTREAMTWOINPUTPROCESSORFACTORY_H

#include "OmniStreamMultipleInputProcessor.h"
#include "OmniStreamTaskNetworkInputFactory.h"
#include "partition/consumer/InputGate.h"
#include "operators/AbstractTwoInputStreamOperator.h"
#include "tasks/OmniStreamTaskNetworkOutput.h"


namespace omnistream {
    class OmniStreamTwoInputProcessorFactory {
    public:
        static std::shared_ptr<OmniStreamMultipleInputProcessor> create(OperatorChainV2* operatorChain, std::vector<std::shared_ptr<InputGate>> inputGates,
                                                        AbstractTwoInputStreamOperator* streamOperator)
        {
            std::vector<OmniStreamOneInputProcessor*> processors;
            OmniStreamTaskInput *input1 = OmniStreamTaskNetworkInputFactory::create(
                0, inputGates[0]);
            auto counter = streamOperator->GetMectrics()->GetInternalOperatorIOMetric(streamOperator->getTypeName(), "numRecordsIn");
            OmniStreamTaskInput *input2 = reinterpret_cast<OmniStreamTaskInput *>(OmniStreamTaskNetworkInputFactory::create(1 ,inputGates[1]));
            omnistream::OmniStreamTwoInputProcessorFactory::OmniStreamTaskNetworkOutPut *pOutPut1 = new OmniStreamTaskNetworkOutPut(
                    streamOperator, [](StreamRecord *record, AbstractTwoInputStreamOperator* streamOperator) {
                        streamOperator->processBatch1(record);
                    }, 0, reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter));

            omnistream::OmniStreamTwoInputProcessorFactory::OmniStreamTaskNetworkOutPut *pOutPut2 = new OmniStreamTaskNetworkOutPut(
                    streamOperator, [](StreamRecord *record,  AbstractTwoInputStreamOperator* streamOperator) {
                        streamOperator->processBatch2(record);
                    }, 1, reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter));

            processors.push_back(new OmniStreamOneInputProcessor(input1, pOutPut1, operatorChain));
            processors.push_back(new OmniStreamOneInputProcessor(input2, pOutPut2, operatorChain));
            return std::make_shared<OmniStreamMultipleInputProcessor>(std::move(processors), std::make_shared<MutipleInputSelectionHandler>(2));
        }
    private:
        class OmniStreamTaskNetworkOutPut : public OmniPushingAsyncDataInput::OmniDataOutput {
        public:
            using RecordConsumer = std::function<void(StreamRecord*, AbstractTwoInputStreamOperator* streamOperator)>;

            OmniStreamTaskNetworkOutPut(AbstractTwoInputStreamOperator *streamOperator, RecordConsumer consumer,
                                        int32_t inputIndex, std::shared_ptr<omnistream::SimpleCounter> & numRecordsIn) :
                streamOperator(streamOperator), consumer_(consumer), inputIndex(inputIndex), numRecordsIn(numRecordsIn)
            {}

            void emitRecord(StreamRecord *streamRecord) override
            {
                if (numRecordsIn != nullptr) {
                    numRecordsIn->Inc();
                }
                consumer_(streamRecord, streamOperator);
            }

            void emitWatermark(Watermark* watermark) override
            {
                LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermark")
                if (inputIndex == 0) {
                    streamOperator->ProcessWatermark1(watermark);
                } else {
                    streamOperator->ProcessWatermark2(watermark);
                }
            }

            void emitWatermarkStatus(WatermarkStatus *watermarkStatus) override
            {
                LOG(">>>> OmniStreamTaskNetworkOutput.emitWatermarkStatus")
            }

            ~OmniStreamTaskNetworkOutPut() override = default;
        private:
            AbstractTwoInputStreamOperator* streamOperator;
            RecordConsumer consumer_;
            int64_t inputIndex;
            std::shared_ptr<omnistream::SimpleCounter> numRecordsIn;
        };
    };

}


#endif //OMNISTREAM_OMNISTREAMTWOINPUTPROCESSORFACTORY_H
