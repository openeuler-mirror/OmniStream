/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "OmniOneInputStreamTask.h"
#include "io/StreamTaskNetworkInputFactory.h"
#include "io/StreamOneInputProcessor.h"
#include "io/OmniPushingAsyncDataInput.h"
#include "io/OmniStreamTaskNetworkInput.h"
#include "io/OmniStreamTaskNetworkInputFactory.h"
#include "task/StreamTaskNetworkOutput.h"
#include "io/OmniStreamOneInputProcessor.h"
#include "OmniStreamTaskNetworkOutput.h"

namespace omnistream {


    // temporary set parameter type is int
    OmniPushingAsyncDataInput::OmniDataOutput *OmniOneInputStreamTask::createDataOutput(
        std::shared_ptr<omnistream::SimpleCounter> & numRecordsIn)
    {
        return new OmniStreamTaskNetworkOutput(dynamic_cast<OneInputStreamOperator*>(mainOperator_), numRecordsIn);
    }

    OmniStreamTaskInput* OmniOneInputStreamTask::createTaskInput() {
        return OmniStreamTaskNetworkInputFactory::create(0, env_->inputGates1()[0]);
    }


    void OmniOneInputStreamTask::init()
    {
        OmniStreamTask::init();
        auto counter = env_->taskMetricGroup()->GetInternalOperatorIOMetric(
            mainOperator_->getTypeName(), "numRecordsOut");
        auto output = createDataOutput(reinterpret_cast<std::shared_ptr<omnistream::SimpleCounter> &>(counter));
        auto input = createTaskInput();

        inputProcessor_ = std::make_shared<OmniStreamOneInputProcessor>(input, output, operatorChain);
    }

    void OmniOneInputStreamTask::processInput(std::shared_ptr<MailboxDefaultAction::Controller> controller)
    {
        // LOG(">>>>OmniOneInputStreamTask::processInput")
        OmniStreamTask::processInput(controller);

        #ifdef TROUBLE_SHOOTING

        //assume this is sink
        LOG_TRACE( taskName_ << "   Sink  Slow down  thefor 1 second to trigger the back pressure")
        std::this_thread::sleep_for(std::chrono::seconds(2));

        #endif

    }

    const std::string OmniOneInputStreamTask::getName() const
    {
        return std::string("OmniOneInputStreamTask");
    }

    void OmniOneInputStreamTask::cleanup()
    {
        OmniStreamTask::cleanup();
        inputProcessor_->close();
    }

}