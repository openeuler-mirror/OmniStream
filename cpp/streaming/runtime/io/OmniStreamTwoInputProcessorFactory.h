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

#ifndef OMNISTREAM_OMNISTREAMTWOINPUTPROCESSORFACTORY_H
#define OMNISTREAM_OMNISTREAMTWOINPUTPROCESSORFACTORY_H

#include "OmniStreamMultipleInputProcessor.h"
#include "OmniStreamTaskNetworkInputFactory.h"
#include "partition/consumer/InputGate.h"
#include "streaming/api/operators/TwoInputStreamOperator.h"
#include "streaming/runtime/io/OmniStreamTaskNetworkOutput.h"
#include "core/typeinfo/TypeInfoFactory.h"


namespace omnistream {
    class OmniStreamTwoInputProcessorFactory {
    public:
        static std::shared_ptr<OmniStreamMultipleInputProcessor> create(OperatorChainV2* operatorChain,
                                                                        std::vector<std::shared_ptr<CheckpointedInputGate>> inputGates,
                                                                        TwoInputStreamOperator* streamOperator,
                                                                        int taskType, const json &description)
        {
            // 1. Create Input
            std::vector<OmniStreamOneInputProcessor*> processors;

            std::vector<long> channelInfoIndex1;
            // Copy elements from JSON array to C++ array
            auto channelInfos1 = inputGates[0]->GetChannelInfos();
            channelInfoIndex1.reserve(channelInfos1.size());
            for (size_t i = 0; i < channelInfos1.size(); ++i) {
                channelInfoIndex1.push_back(static_cast<long>(channelInfos1[i].getInputChannelIdx()));
            }

            std::vector<long> channelInfoIndex2;
            // Copy elements from JSON array to C++ array
            auto channelInfos2 = inputGates[1]->GetChannelInfos();
            channelInfoIndex2.reserve(channelInfos2.size());
            for (size_t i = 0; i < channelInfos2.size(); ++i) {
                channelInfoIndex2.push_back(static_cast<long>(channelInfos2[i].getInputChannelIdx()));
            }

            OmniStreamTaskInput *input1 = nullptr;
            OmniStreamTaskInput *input2 = nullptr;

            if (taskType == 1) {
                auto leftTypes = description["leftInputTypes"].get<std::vector<std::string>>();
                auto rightTypes = description["rightInputTypes"].get<std::vector<std::string>>();

                input1 = OmniStreamTaskNetworkInputFactory::create(0, inputGates[0], taskType,
                                                                   new BinaryRowDataSerializer(leftTypes.size(), leftTypes), channelInfoIndex1);
                input2 = OmniStreamTaskNetworkInputFactory::create(1, inputGates[1], taskType,
                                                                   new BinaryRowDataSerializer(rightTypes.size(), rightTypes), channelInfoIndex2);
            } else if (taskType == 2) {
                auto inputTypes = description["inputTypes"];
                TypeInformation* inputTypeInfo1 = nullptr;
                TypeInformation* inputTypeInfo2 = nullptr;

                if (inputTypes.size() < 2) {
                    THROW_LOGIC_EXCEPTION("Invalid channel input index.");
                }
                if (!inputTypes[0].is_object()) {
                    THROW_LOGIC_EXCEPTION("Invalid channel input index.");
                }
                inputTypeInfo1 = TypeInfoFactory::createDataStreamTypeInfo(inputTypes[0]);
                if (!inputTypes[1].is_object()) {
                    THROW_LOGIC_EXCEPTION("Invalid channel input index.");
                }
                inputTypeInfo2 = TypeInfoFactory::createDataStreamTypeInfo(inputTypes[1]);

                input1 = OmniStreamTaskNetworkInputFactory::create(0, inputGates[0], taskType,
                                                                   inputTypeInfo1 == nullptr ? nullptr : inputTypeInfo1->createTypeSerializer(), channelInfoIndex1);
                if (inputTypeInfo1 != nullptr) {
                    delete inputTypeInfo1;
                }

                input2 = OmniStreamTaskNetworkInputFactory::create(1, inputGates[1], taskType,
                                                                   inputTypeInfo2 == nullptr ? nullptr : inputTypeInfo2->createTypeSerializer(), channelInfoIndex2);
                if (inputTypeInfo2 != nullptr) {
                    delete inputTypeInfo2;
                }
            }

            if (!streamOperator->GetMectrics()) {
                INFO_RELEASE("metric is null")
                throw std::runtime_error("metric is null");
            }
            auto counter = streamOperator->GetMectrics()->GetInternalOperatorIOMetric(streamOperator->getTypeName(), "numRecordsIn");

            // 2. CreateOutput
            omnistream::OmniStreamTwoInputProcessorFactory::OmniStreamTaskNetworkOutPut *pOutPut1 = nullptr;
            omnistream::OmniStreamTwoInputProcessorFactory::OmniStreamTaskNetworkOutPut *pOutPut2 = nullptr;
            if (taskType == 1) {
                pOutPut1 = new OmniStreamTaskNetworkOutPut(
                        streamOperator, [](StreamRecord *record, TwoInputStreamOperator* streamOperator) {
                            streamOperator->processBatch1(record);
                        }, 0, reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter));
                pOutPut2 = new OmniStreamTaskNetworkOutPut(
                        streamOperator, [](StreamRecord *record,  TwoInputStreamOperator* streamOperator) {
                            streamOperator->processBatch2(record);
                        }, 1, reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter));
            } else if (taskType == 2) {
                pOutPut1 = new OmniStreamTaskNetworkOutPut(
                        streamOperator, [](StreamRecord *record, TwoInputStreamOperator* streamOperator) {
                            streamOperator->setKeyContextElement1(record);
                            streamOperator->processElement1(record);
                        }, 0, reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter));

                pOutPut2 = new OmniStreamTaskNetworkOutPut(
                        streamOperator, [](StreamRecord *record,  TwoInputStreamOperator* streamOperator) {
                            streamOperator->setKeyContextElement2(record);
                            streamOperator->processElement2(record);
                        }, 1, reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter>&>(counter));
            } else {
                THROW_LOGIC_EXCEPTION("Invalid task type in creating OmniStreamTwoInputProcessor.");
            }

            processors.push_back(new OmniStreamOneInputProcessor(input1, pOutPut1, operatorChain));
            processors.push_back(new OmniStreamOneInputProcessor(input2, pOutPut2, operatorChain));
            return std::make_shared<OmniStreamMultipleInputProcessor>(std::move(processors), std::make_shared<MutipleInputSelectionHandler>(2));
        }
    private:
        class OmniStreamTaskNetworkOutPut : public OmniPushingAsyncDataInput::OmniDataOutput {
        public:
            using RecordConsumer = std::function<void(StreamRecord*, TwoInputStreamOperator* streamOperator)>;

            OmniStreamTaskNetworkOutPut(TwoInputStreamOperator *op, RecordConsumer consumer,
                int32_t inputIndex, std::shared_ptr<omnistream::SimpleCounter> & numRecordsIn)
                : streamOperator(op), consumer_(consumer), inputIndex(inputIndex), numRecordsIn(numRecordsIn)
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
            TwoInputStreamOperator* streamOperator;
            RecordConsumer consumer_;
            int64_t inputIndex;
            std::shared_ptr<omnistream::SimpleCounter> numRecordsIn;
        };
    };

}


#endif
