/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
//
// Created by root on 3/9/25.
//

#include "OmniTwoInputStreamTask.h"
#include "io/OmniStreamTwoInputProcessorFactory.h"

namespace omnistream {
    void OmniTwoInputStreamTask::init() {
        auto pod = env_->taskConfiguration().getStreamConfigPOD();
        int32_t numberOfInputs = pod.getNumberOfNetworkInputs();
        std::vector<std::shared_ptr<InputGate>> inputList1;
        std::vector<std::shared_ptr<InputGate>> inputList2;
        const std::vector<StreamEdgePOD> &inEdges = pod.getInStreamEdges();
        for (int i =0; i < numberOfInputs; i++) {
            int inputType = inEdges[i].getTypeNumber();
            std::shared_ptr<InputGate> ptr = env_->inputGates1()[i];
            switch (inputType) {
                case 1:
                    inputList1.push_back(ptr);
                    break;
                case 2:
                    inputList2.push_back(ptr);
                    break;
                default:
                    THROW_RUNTIME_ERROR("Invalid input type number:" + std::to_string(inputType))
            }
        }
        createInputProcessor(inputList1, inputList2);
    }

    void OmniTwoInputStreamTask::createInputProcessor(std::vector<std::shared_ptr<InputGate>> inputGates1,
                                                      std::vector<std::shared_ptr<InputGate>> inputGates2) {
        // here we not care about unionInputGate
        inputGates1.insert(inputGates1.end(), std::make_move_iterator(inputGates2.begin()),
                           std::make_move_iterator(inputGates2.end()));
        inputProcessor_ = OmniStreamTwoInputProcessorFactory::create(operatorChain, inputGates1,
                                                                     static_cast<AbstractTwoInputStreamOperator*>(mainOperator_));
    }

}
